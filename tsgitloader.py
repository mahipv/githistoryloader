import psycopg2
from dotenv import find_dotenv, load_dotenv
import os
import shutil
import subprocess
from git import Repo
import threading
import pandas as pd
from toolchainutils import LangChain, LlamaIndex

MAX_THREAD_COUNT = 6
TSV_TM_CATALOG_TABLE_NAME = "time_machine_catalog"
SCRATCH_REPO_DIR = "temprepo"
DEFAULT_TOOL_CHAIN = "langchain"

def github_url_to_table_name(github_url, toolchain):
    repository_path = github_url.replace("https://github.com/", "")
    table_name = toolchain+"_"+repository_path.replace("/", "_")
    return table_name

def record_catalog_info(repo_url, branch, toolchain):
    with psycopg2.connect(dsn=os.environ["TIMESCALE_SERVICE_URL"]) as connection:
        # Create a cursor within the context manager
        with connection.cursor() as cursor:
            # Define the Git catalog table creation SQL command
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {TSV_TM_CATALOG_TABLE_NAME} (
                repo_url TEXT,
                table_name TEXT,
                tool_chain TEXT,
                PRIMARY KEY(repo_url, tool_chain)
            );
            """
            cursor.execute(create_table_sql)

            delete_sql = f"DELETE FROM {TSV_TM_CATALOG_TABLE_NAME} WHERE repo_url = %s AND tool_chain = %s" 
            cursor.execute(delete_sql, (repo_url, toolchain))

            insert_data_sql = """
            INSERT INTO time_machine_catalog (repo_url, table_name, tool_chain)
            VALUES (%s, %s, %s);
            """
            
            table_name = github_url_to_table_name(repo_url, toolchain)
            cursor.execute(insert_data_sql, (repo_url, table_name, toolchain))
            connection.commit()
            return table_name
        
def git_clone_url(repo_url, branch, tmprepo_dir):
    # Check if the clone directory exists, and if so, remove it
    if os.path.exists(tmprepo_dir):
        shutil.rmtree(tmprepo_dir)
    os.makedirs(tmprepo_dir)
    try:
        # Clone the Git repository with the specified branch
        res = subprocess.run(
            [
                "git",
                "clone",
                "--filter=blob:none",
                "--no-checkout",
                "--single-branch",
                "--branch=" + branch,
                repo_url + ".git",
                tmprepo_dir,
            ],
            capture_output=True,
            text=True,
            cwd=".",  # Set the working directory here
        )

        if res.returncode != 0:
            raise ValueError(f"Git failed: {res.returncode}")
        return tmprepo_dir
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")


def process_commit_range(repo_dir, commit_count, skip_count, toolchains):
    repo = Repo(repo_dir)
    # Create lists to store data
    commit_hashes = []
    authors = []
    dates = []
    subjects = []
    bodies = []
    print(f"{threading.current_thread().name} commits:{commit_count} - skip : {skip_count}")
    # Iterate through commits and collect data
    for commit in repo.iter_commits(max_count=commit_count, skip=skip_count):
        commit_hash = commit.hexsha
        author = commit.author.name
        date = commit.committed_datetime.isoformat()
        message_lines = commit.message.splitlines()
        subject = message_lines[0]
        body = "\n".join(message_lines[1:]) if len(message_lines) > 1 else ""

        commit_hashes.append(commit_hash)
        authors.append(author)
        dates.append(date)
        subjects.append(subject)
        bodies.append(body)

    # Create a DataFrame from the collected data
    data = {
        "Commit Hash": commit_hashes,
        "Author": authors,
        "Date": dates,
        "Subject": subjects,
        "Body": bodies
    }
    df = pd.DataFrame(data)
    df.dropna(inplace=True)
    df = df.astype(str)
    df = df.applymap(lambda x: x.strip('"'))
    for toolchain in toolchains:
        rows = toolchain.process_frame(df)
        toolchain.insert_rows(rows)
    #print(df.iloc[[0, -1]])

def setup_tables(repourl, branch, tool_chains) -> any:
    tool_chain_list = tool_chains.split(",")
    toolchains = []
    for toolchain in tool_chain_list:
        if toolchain == "langchain":
            toolchain_obj = LangChain(record_catalog_info(repourl, branch, toolchain))
        if toolchain == "llamaindex":
            toolchain_obj =LlamaIndex(record_catalog_info(repourl, branch,toolchain))
        print(toolchain_obj.get_table_name())
        toolchain_obj.create_tables()
        toolchains.append(toolchain_obj)
    return toolchains

def multi_load(repo_url, branch="master", tool_chain="langchain,llamaindex"):
    repo_dir = git_clone_url(repo_url, branch, SCRATCH_REPO_DIR)
    #repo_dir = tsg.SCRATCH_REPO_DIR
    repo = Repo(repo_dir)
    commit_count = len(list(repo.iter_commits()))
    print(f"Commit count: {commit_count}")
    commits_per_thread = commit_count//(MAX_THREAD_COUNT)
    remainder = commit_count % MAX_THREAD_COUNT
    thread_workloads = [commits_per_thread] * (MAX_THREAD_COUNT - 1) + [commits_per_thread + remainder]
    print(thread_workloads)
    skip = 0
    threads = []
    toolchains = setup_tables(repo_url, branch, tool_chain)
    for thread_commit_count in thread_workloads:
        name = f"Thread_skip_{skip}_{thread_commit_count}"
        thread = threading.Thread(target=process_commit_range, name=name, args=(repo_dir, thread_commit_count, skip, toolchains))
        skip+= thread_commit_count
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
    
def read_catalog_info(toolchain=DEFAULT_TOOL_CHAIN)->any:
    with psycopg2.connect(dsn=os.environ["TIMESCALE_SERVICE_URL"]) as connection:
        # Create a cursor within the context manager
        with connection.cursor() as cursor:
            try:
                select_data_sql = f"SELECT repo_url, table_name FROM time_machine_catalog WHERE tool_chain = \'{toolchain}\'"
                print(select_data_sql)
                cursor.execute(select_data_sql)
            except psycopg2.errors.UndefinedTable as e:
                return {}

            catalog_entries = cursor.fetchall()

            catalog_dict = {}
            for entry in catalog_entries:
                repo_url, table_name = entry
                catalog_dict[repo_url] = table_name

            return catalog_dict

def load_git_history(repo_url:str, branch:str, toolchain:str):
    multi_load(repo_url, branch, toolchain)
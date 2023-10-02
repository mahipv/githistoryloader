# githistoryloader
Load git commit history into a TS embeddings table

## Usage

```
from tsgitloader import load_git_history, read_catalog_info
from dotenv import load_dotenv, find_dotenv

# set os.environ["TIMESCALE_SERVICE_URL"] and os.environ['OPENAI_API_KEY'] from the .env file
_ = load_dotenv(find_dotenv())

load_git_history("https://github.com/timescale/timescaledb", "main", "llamaindex")
load_git_history("https://github.com/timescale/timescaledb", "main", "langchain")

print(read_catalog_info("llamaindex"))
print(read_catalog_info("langchain"))
```
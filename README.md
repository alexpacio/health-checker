# Health Checker

The Health Checker is a program that allows you to massively check and record the availability of any kind of target. Currently only HTTP(s) is supported as a protocol.

# Environment bootstrap

This program has been tested on Windows 11 running on AMD64 and Python 3.13.2. The following commands follow the Windows path style and are only tested on Powershell.

## Setup guide

### Setup the environment variables

Copy the ".env.example" file that you can find inside the project code folder named "website_monitor" to ".env". That's going to be the file where the runtime settings has to be stored.
Refer to src\runtime_settings.py to check out the list of parameters that can be set, their boundaries and default values if any or in case they are optional.

### Setting the environment for the first time
```
python -m venv venv
venv\Scripts\activate
cd website_monitor
pip install -r requirements.txt
```

### Initialize the database schema
```
python .\scripts\apply_schema.py
```

### Create new targets
Use the below CLI program to create new HTTP targets.
The CLI program can be used to interact with the health checker's settings.

Here's an example of how the CLI can be used:
```
# Add a new healthcheck setting
python healthcheck_db_setup.py add --url=https://example.com/path [--regex-match="Success"] [--expected-status-code=200] 
                                    [--check-interval=60] [--timeout-ms=5000] 
                                    [--headers='{"User-Agent": "MyBot"}'] [--active=true]

# List all healthcheck settings (with filters)
python healthcheck_db_setup.py list [--active-only] [--url-filter=example.com]

# Update existing healthcheck settings
python healthcheck_db_setup.py update --id=1 [--url=https://new-url.com] [--regex-match="NewPattern"] 
                                        [--active=false] [--check-interval=120] [--timeout-ms=10000]
                                        [--headers='{"User-Agent": "MyBot"}'] [--expected-status-code=200]

# Delete healthcheck settings
python healthcheck_db_setup.py delete --id=1
```

Now, with the purpose of moving on with your own playground, just move on by adding sample data like this:

```
python scripts\healthcheck_db_setup.py add --url=http://127.0.0.1:8080/a --check-interval=5
python scripts\healthcheck_db_setup.py add --url=http://127.0.0.1:8080/b --check-interval=10
python scripts\healthcheck_db_setup.py add --url=http://127.0.0.1:8080/c --check-interval=300
```

### Spawn a webserver serving multiple routes
Run this on a separate and dedicated terminal or in background.

```
python .\scripts\async_web_server.py --host=127.0.0.1 --port=8080 --routes=/a,/b,/c
```

### (Optional) Run the visualizer to see the health checker results graphically

```
python .\scripts\visualizer.py
```

### Run the program

```
python .\src\run.py
```

## Run tests

Note: the test_db_recorder.py test suite is an E2E test and requires a PostgreSQL database where to write data. Make sure the target is transient!

```
python -m unittest discover -s tests
```

## Caveats

The regex match feature works best with text-based responses, such as HTML, JSON, XML, plain text, etc. For binary content (like images, PDFs, etc.), the regex match will fail with a specific error message indicating that regex matching can't be performed on binary content.
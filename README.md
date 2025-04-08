# Health Checker

The Health Checker is a program that allows you to massively check and record the availability of any kind of target. Currently only HTTP(s) is supported as a protocol.

## Environment bootstrap

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
Use the below script to create new HTTP targets. Use this example if you just want to try out the program coupled with the provided webserver.

```
python .\scripts\healthcheck_db_setup.py --base-url http://127.0.0.1:8080 --routes /a,/b,/c --check-interval 5
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
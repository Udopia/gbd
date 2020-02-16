# gbd (Global Benchmark Database)

## Programming Language
Python 3

## Installation

- Setup python3 and pip3
- Install GBD via ```pip3 install global-benchmark-database-tool```
- Optional: Download a database, e.g., [https://baldur.iti.kit.edu/gbd/](https://baldur.iti.kit.edu/gbd/roth.db), and safe it under /path/to/db/file.db
- Configure DB Path: ```export GBD_DB=/path/to/db/file.db``` (and put it in your .bashrc)
- Reinitialize paths in "benchmarks" table: ```gbd init /path/to/cnf```

## Test
>   ```gbd get -r benchmarks```

### Using GBD Server
- After getting started, you can use GBD from the command line as explained in the help section
- For starting the server on Linux, run ```sh /server/run_server.sh /path/to/db/file.db```. If no path is given, the script uses the path from ```GBD_DB```
- For starting the server on Windows, run ```\path\to\python3\interpreter \server\server.py -d \path\to\db\file.db```

### Help on basic commands
>	```gbd -h```

### Help on specific command
>	```gbd [command] -h```

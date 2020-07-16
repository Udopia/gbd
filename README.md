# GBD Benchmark Database Tools

## Programming Language
Python 3

## Installation
- Setup python3 and pip3
- Install GBD via ```pip3 install gbd-tools```

## Configuration
- Download a database, e.g., [http://gbd.iti.kit.edu/getdatabase/](http://gbd.iti.kit.edu/getdatabase/), and safe it as [path/file]
- ```export GBD_DB=[path/file]``` (and put it in your .bashrc)
- If no database path is given via --db, then gbd uses path in ```GBD_DB```

## Initialize local paths to benchmark instances
> ```gbd init /path/to/cnf```

## GBD Command Line Interface
> ```gbd --help```

### Help on specific command
>	```gbd [command] --help```

### Using GBD Server and Microservices
> ```gbd-server --help```.

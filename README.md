# CarbonTrader blockchain

--Insert description here --

# Instructions

## Prerequisites

- This program must be run in a linux or unix machine, it can also be run in WSL. 
- The should be a file in `/db` named service-account-info.json with the credentials of your google cloud Pub/Sub.
- The machine must have `make` install.
- There must be internet connection.

## Configuration 
- For a fast configuration use the command `make config-linux`. (This only works in Debian systems).

## Running the blockchain node.
- To run the program run the command `make serve`. This will command will do the following:
  - Create a python environment.
  - Install the requirements.
  - Run the blockchain in the python environment.


# EAS Project

It uses the following components:

- **Database connection (db_connection.py)**: Manages connections to the database.
- **Data processing (processing.py)**: Contains functions for processing data.

## Usage

1. **Run the script**: Execute `analysis_by_date.py` to start process.
2. **Configuration**: Ensure the `config.py` file is correctly set up for database access.
3. **Output**: The plots will be saved in the `plots/` directory.

## Project Structure

The project consists of the following files and directories:

```
nevod/
|-- config.py           # Contains configuration settings for the database.
|-- db_connection.py    # Handles database connection logic.
|-- processing.py       # Functions for data processing.
|-- plot_angles.py      # Script to plot the calculated angles.
|-- plots/              # Directory where generated plots are saved.
```

.
# Data Extraction Scripts

## Synapse Data Extraction Script

### Overview
This script extracts data from Synapse and converts it to JSON format. I started with an existing script and added new functions to extract additional data types (e.g., fuel consumption data).

### Tools
- Python
- T-SQL

### Example Code
See `synapse_to_json_extractor.py` for code snippets illustrating the extraction process.

---

## Timescale Data Extraction Script

### Overview
This script extracts data from Timescale and converts it to JSON format. I developed it from scratch, using the Synapse extractor as a reference and AI tools for syntax and debugging support. This project also introduced me to using `.env` files for secure credential management.

### Tools
- Python
- SQL

### Example Code
See `timescale_to_json_extractor.py` for code snippets illustrating the extraction process.


# 📬 WebContactsExtractor: Automated Contact Data Extraction

**WebContactsExtractor** is a powerful and modular Python-based tool designed for automated extraction of email addresses and social media profiles from websites listed in `.csv` files. It streamlines the process of gathering contact information, offering advanced features for data verification, cleaning, and the generation of demo-ready, anonymized datasets.

---

## 🚀 Key Features

-   **Automated Email Extraction**: Leverages Selenium for robust scraping of email addresses from web pages.
-   **Social Media Scraping**: Capable of identifying and extracting links to common social media platforms (e.g., Facebook, Instagram, LinkedIn, X/Twitter).
-   **Advanced Email Verification**: Includes checks for format, domain validity, MX records, SPF, DKIM, and SMTP verification to ensure data quality.
-   **Customizable Email Exclusion**: Allows filtering out emails based on user-defined lists of keywords, names, or spam indicators located in `config/xclusiones_email/`.
-   **Data Anonymization (Demo Generation)**: Generates anonymized versions of your data for demonstration or testing purposes, protecting sensitive information. Output files use a `_demo` suffix and are saved in `data/demo_outputs/`.
-   **Flexible Data Handling**:
    -   **Column Management**: Provides utilities to reorder, rename, or remove columns in your datasets.
    -   **Excel Output**: Organizes extracted and processed data into `.xlsx` files.
-   **Image Capture**: Generates images from statistics tables within `.xlsx` files during certain processes (e.g., email exclusion).
-   **Efficient Processing**:
    -   **Parallel Execution**: Utilizes `ThreadPoolExecutor` for concurrent processing, enhancing performance for large datasets.
    -   **Batch CSV Cleaning**: Includes functionality to clean up empty or irrelevant CSV files in bulk.
-   **Production-Ready Structure**: Designed with a clear and scalable project structure for ease of maintenance and future development.
-   **Checkpoint System**: Implements a checkpoint manager to resume interrupted processes, saving time and resources.
-   **Intelligent Caching**: Uses a cache manager to store results and avoid redundant processing.

---

## 🗂 Project Structure

The project is organized as follows:

```
WebContactsExtractor/
├── src/                           # Main source code
│   ├── main.py                    # Primary script to run the full workflow
│   ├── core/                      # Core components (config, error handling, Excel, etc.)
│   ├── scraping/                  # Web scraping modules
│   ├── exclusion/                 # Email filtering logic
│   ├── masking/                   # Data anonymization (demo generation) logic
│   └── utils/                     # Utility scripts (monitoring, cleanup)
├── config/                        # Configuration files
│   ├── txt_config/                # General text-based configurations
│   └── xclusiones_email/          # Lists for email exclusion
├── data/                          # Data input/output
│   ├── inputs/                    # Input CSV files for main extraction
│   ├── outputs/                   # Output from the main extraction process
│   ├── exclusion_outputs/         # Output from the email exclusion process
│   ├── demo_inputs/               # Input files for demo generation
│   └── demo_outputs/              # Output for demo (anonymized) files
├── logs/                          # Log files
├── drivers/                       # Web drivers (e.g., chromedriver.exe)
├── scripts/                       # Standalone utility scripts
│   ├── demo_masker.py             # Script for generating demo files
│   ├── main_xclusionEmail.py      # Script for email exclusion process
│   └── ficheros_datos.py          # Script for specific data aggregation
├── requirements.txt               # Project dependencies
└── README.md                      # This file
```

---

## ⚙️ Prerequisites & Installation

To get WebContactsExtractor up and running, you'll need:

1.  **Python**: Version 3.8 or higher.
2.  **Google Chrome**: The latest stable version installed on your system.
3.  **ChromeDriver**:
    *   Download the ChromeDriver version that matches your Google Chrome installation from [ChromeDriver Official Site](https://chromedriver.chromium.org/downloads).
    *   Place the `chromedriver.exe` (or equivalent for your OS) into the `drivers/` folder of this project, or ensure it's accessible via your system's `PATH` environment variable.

Once the above are set up, install the required Python packages by running the following command in the project's root directory:

```bash
pip install -r requirements.txt
```

---

## ▶️ How to Use

### 1. Main Data Extraction Workflow

This is the primary way to use the tool, automating the entire process from scraping to generating demo files.

1.  Place your input `.csv` files (containing website URLs or company names for scraping) into the `data/inputs/` directory.
2.  Run the main script from the project's root directory:
    ```bash
    python -m src.main
    ```
3.  The complete process will execute the following stages:
    *   **Extraction**: Scrapes websites for emails and social media links. Results are saved in `data/outputs/`.
    *   **Exclusion**: Filters out unwanted emails based on your criteria in `config/xclusiones_email/`. Results are saved in `data/exclusion_outputs/`.
    *   **Demo Data Generation**: Anonymizes sensitive data from the extraction process, creating demo-ready files. Results are saved in `data/demo_outputs/` with a `_demo` suffix.

### 2. Generating Demo (Anonymized) Files Standalone

If you need to create anonymized versions of existing datasets (e.g., for sharing or testing without exposing real data):

1.  Place your `.csv` or `.xlsx` files containing sensitive data into the `data/demo_inputs/` directory.
2.  Execute the `demo_masker.py` script:
    ```bash
    python scripts/demo_masker.py
    ```
3.  Anonymized files, suffixed with `_demo` (e.g., `original_file_demo.xlsx`), will be generated in the `data/demo_outputs/` directory. Example transformations:
    ```
    contact@company.com  ->  c******@company.com
    +1 612 345 67 89     ->  +1 612 345 ** **
    instagram.com/user   ->  instagram.com/****
    ```

### 3. Email Exclusion Process (Standalone)

To run only the email exclusion process on existing files:

1.  Ensure your exclusion lists (`apellidos.txt`, `nombres.txt`, `spam.txt`) in `config/xclusiones_email/` are up-to-date.
2.  Place the files you want to process into the appropriate input directory (e.g., `data/outputs/` if they are already extracted).
3.  Run the `main_xclusionEmail.py` script:
    ```bash
    python scripts/main_xclusionEmail.py
    ```
    This script will filter the emails and save the results in `data/exclusion_outputs/`. Additionally, this script may generate images based on the statistics of the processed `.xlsx` files.

### 4. Specific Data Aggregation (`ficheros_datos.py`)

This script is designed for a specialized task: it navigates through country-specific folders within a specified path, locates Excel files, extracts specific metrics from a "statistics" sheet, associates available JPG images, and compiles a summary Excel file.

1.  Configure the script `scripts/ficheros_datos.py` with the correct paths and parameters as needed.
2.  Execute the script:
    ```bash
    python scripts/ficheros_datos.py
    ```
3.  The aggregated data will be saved as an Excel file, typically in `data/outputs/` or a path specified within the script.

---

## 📝 Notes

-   Ensure all paths in configuration files and scripts are correctly set for your environment.
-   Regularly update your ChromeDriver to match your Chrome browser version to avoid compatibility issues.
-   For large-scale scraping, be mindful of website terms of service and ethical considerations. Implement appropriate delays and user-agent rotation if necessary.

---

## 🤝 Contributing

Contributions are welcome! If you have suggestions for improvements or new features, please feel free to open an issue or submit a pull request.

---

## 📄 License

This project is licensed under the MIT License. (If you don't have a LICENSE.md file with the MIT License, you should add one or adjust this section accordingly).

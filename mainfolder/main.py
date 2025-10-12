import os
import re
import sys
import pandas as pd
from pathlib import Path
import yaml

class ANIProcessor:
    def __init__(self, input_folder, output_folder, temp_csv):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.temp_csv = temp_csv
        self.file_info = None
        if os.path.exists(self.temp_csv):                   # Remove temp CSV if it exists
            os.remove(self.temp_csv)
        self.month_map = {
            "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
            "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
            "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
        }
        self.folder_name = os.path.basename(os.path.normpath(self.input_folder))
        folder_match = re.match(r'([A-Za-z]+)-(\d{4})$', self.folder_name)
        if not folder_match:
            raise ValueError(
                f"❌ Folder name '{self.folder_name}' does not match expected format 'Mon-YYYY' "
                f"(e.g., 'Jun-2025'). Please rename the folder and try again."
            )
        month_text, self.folder_year = folder_match.groups()                # Extract year and month from folder name
        self.month_text_cap = month_text.capitalize()               

        if self.month_text_cap not in self.month_map:
            raise ValueError(f"❌ Invalid month name '{self.month_text_cap}''{month_text}' in folder name. Use three-letter English month abbreviations (e.g. Jan, Feb, Mar).")

        self.folder_month = self.month_map[self.month_text_cap]

    def parse_filename(self, filename):
        filename_no_ext = os.path.splitext(filename)[0].strip()                 # Remove extension        
        branch_org_code = filename_no_ext.split("_FS")[0]                       # Extract BranchOrgCode (before "_FS")                                    
        match = re.search(r'_FS[\s_]*?(\d{2})-(\d{2})', filename_no_ext)        
        if not match:
            print(f"Filename '{filename}' does not match expected pattern")     # if the filename is in the wrong format, skip this file
            return 
        month = self.folder_month
        year = self.folder_year                                   
        docu_date = f"01/{month}/{year}"
        return {
            "FileName": filename,
            "OrgCode": "ANI",
            "BranchOrgCode": branch_org_code,
            "DocuDate": docu_date,
            "DateYear": year,
            "DateMonth": month
        }


    def process_data(self, file, company_info):
        sheet_name = self.month_text_cap
        if sheet_name is None:
            raise ValueError(f"❌ No matching sheet found for month '{company_info['DateMonth']}'")

        df = pd.read_excel(file, sheet_name=sheet_name)

        # --- Find starting row (below "Account No.") ---
        header_row = None
        for i, row in df.iterrows():
            if "Account No." in row.values:
                header_row = i
                break
        if header_row is None:
            print(f"Skipping {file} — 'Account No.' not found")             # if header row('Account No.' ) is not found, skip this file
            return
        df.columns = df.iloc[header_row].astype(str).str.strip()
        data = df.iloc[header_row+1:].reset_index(drop=True)

        required_cols = ["Account No.", "Account Description", "THB"]

        missing_cols = [col for col in required_cols if col not in data.columns]
        if not missing_cols:   
            data = data.rename(columns={
                "Account No.": "AccCode",
                "Account Description": "AccName",
                "THB": "AcccumMonthAmnt"
            })
        else:
            print(f"Skipping {file} — missing columns: {', '.join(missing_cols)}")   # if required columns are not found, skip this file.
            return 


        data = data[["AccCode", "AccName", "AcccumMonthAmnt"]]
        data = data[data["AccCode"].notna() & (data["AccCode"].astype(str).str.strip() != "")]      # Remove rows where AccCode is empty
        data.insert(0, "OrgCode", company_info["OrgCode"])
        data.insert(1, "BranchOrgCode", company_info["BranchOrgCode"])
        data.insert(2, "DocuDate", company_info["DocuDate"])
        data.insert(3, "DateYear", company_info["DateYear"])
        data.insert(4, "DateMonth", company_info["DateMonth"])

        # --- Append to CSV ---
        write_header = not os.path.exists(self.temp_csv)
        data.to_csv(self.temp_csv, index=False, mode="a", header=write_header)


    def convert_csv_to_xlsx(self):
        export_name = f"ANI-{self.file_info['DateYear']}-{self.file_info['DateMonth']}"
        final_excel = os.path.join(self.output_folder, f"{export_name}.xlsx")
        final_df = pd.read_csv(self.temp_csv)
        final_df.to_excel(final_excel, index=False, sheet_name=export_name)
        print(f"✅ Final Excel file saved as: {final_excel}")


    def run(self):
        for file in os.listdir(self.input_folder):
            print(file)
            if not file.endswith(".xlsx"):                                   # if the file is not an .xlsx, skip this file.
                continue

            file_path = os.path.join(self.input_folder, file)
            company_info = self.parse_filename(file)

            if company_info is None:                                        # if the filename is in the wrong format, skip this file.
               continue

            self.process_data(file_path, company_info)
            self.file_info = company_info                                   # save last file info for naming Excel
        if self.file_info:
            self.convert_csv_to_xlsx()

def load_config():
    config_path = Path("config.yaml")
    if not config_path.exists():
        print("❌ ERROR: config.yaml not found. Navigate to folder with config.yaml before running the app.")
        sys.exit(1)   # exit with error code 1
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    # === Folder with Excel input files ===
    config = load_config()
    input_folder  = Path(config["paths"]["input_folder"])
    output_folder = Path(config["paths"]["output_folder"])
    temp_csv  = Path(config["paths"]["temp_data"])

    for subfolder in sorted(input_folder.iterdir()):
        if subfolder.is_dir():     # process all month folders that match pattern "Jan-YYYY"  (ignore case-insensitive)      
            if re.match(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{4}$", subfolder.name, re.IGNORECASE):
                processor = ANIProcessor(subfolder, output_folder, temp_csv)
                processor.run()
            else:
                print(f"⚠️ Skipping folder (wrong format): {subfolder.name}")

    if not input_folder.exists():
        print("❌ ERROR: input_folder not found. Please edit config.yaml.")
        return
    if not output_folder.exists():
        print("❌ ERROR: output_folder not found. Please edit config.yaml.")
        return


if __name__ == "__main__":
    main()

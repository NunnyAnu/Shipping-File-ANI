import os
import re
import pandas as pd

class ANIProcessor:
    def __init__(self, input_folder, output_folder, temp_csv):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.temp_csv = temp_csv
        self.file_info = None
        if os.path.exists(self.temp_csv):                   # Remove temp CSV if it exists
            os.remove(self.temp_csv)
        self.month_map = {
            "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
            "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
            "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec"
        }

    def parse_filename(self, filename):
        filename_no_ext = os.path.splitext(filename)[0].strip()         # Remove extension        
        org_code = filename_no_ext.split("_FS")[0]                      # Extract OrgCode (before "_FS")
        branch_org_code = org_code                                      
        match = re.search(r'_FS[\s_]*?(\d{2})-(\d{2})', filename_no_ext) # Extract year and month
        if not match:
            print(f"Filename '{filename}' does not match expected pattern") # if the filename is in the wrong format, skip this file
            return
        year_suffix, month = match.groups()
        year = "20" + year_suffix                                        # millennium 20xx
        docu_date = f"01/{month}/{year}"
        return {
            "FileName": filename,
            "OrgCode": org_code,
            "BranchOrgCode": branch_org_code,
            "DocuDate": docu_date,
            "DateYear": year,
            "DateMonth": month
        }


    def process_data(self, file, company_info, temp_csv):
        sheet_name = self.month_map.get(company_info["DateMonth"], None)
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
        write_header = not os.path.exists(temp_csv)
        data.to_csv(self.temp_csv, index=False, mode="a", header=write_header)


    def convert_csv_to_xlsx(self, temp_csv, output_folder, year, month):
        final_excel = os.path.join(self.output_folder, f"ANI-{year}-{month}.xlsx")
        final_df = pd.read_csv(self.temp_csv)
        final_df.to_excel(final_excel, index=False)
        print(f"✅ Final Excel file saved as: {final_excel}")


    def run(self):
        for file in os.listdir(self.input_folder):
            print(file)
            if not file.endswith(".xlsx"):                                   # if the file is not an .xlsx, skip this file.
                continue

            file_path = os.path.join(self.input_folder, file)
            company_info = self.parse_filename(file)
            # print(company_info)

            if company_info is None:                                        # if the filename is in the wrong format, skip this file.
               continue

            self.process_data(file_path, company_info, temp_csv)
            self.file_info = company_info                                                                # save last file info for naming Excel
        if self.file_info:
            self.convert_csv_to_xlsx(temp_csv, output_folder, self.file_info["DateYear"], self.file_info["DateMonth"])


if __name__ == "__main__":
    input_folder = "/Users/nunny/Desktop/Shipping-File-ANI/mainfolder/input-folder/Jun-2025"
    output_folder = "/Users/nunny/Desktop/Shipping-File-ANI/mainfolder/output"
    temp_csv = "/Users/nunny/Desktop/Shipping-File-ANI/mainfolder/temp_data.csv"
    processor = ANIProcessor(input_folder, output_folder, temp_csv)
    processor.run()

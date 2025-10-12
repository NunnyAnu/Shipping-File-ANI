# Shipping-File-ANI

## Quick Usage
1. Put the file(s) you want to process into the `mainfolder/input-folder` (you can add more than one folder).
2. Edit Input and Output Path in ```config.yaml```.
3. Run or Double-click `mainfolder/MainApp.exe`.
4. The processed file(s) will appear in the `mainfolder/output`.

## Building .exe


### 1. Clone this repository
```
git clone https://github.com/NunnyAnu/Shipping-File-ANI
```
### 2. Navigate into the working folder

```
cd Shipping-File-ANI/mainfolder
```

### 3. Create a new Python environment 
#### With Python VENV
```
python -m venv shipenv
source shipenv/bin/activate   # (Linux / macOS)
shipenv\Scripts\activate      # (Windows PowerShell / CMD)
```

#### With Conda
```
conda create -n shipenv python=3.11 pip
conda activate shipenv
```

### 4. Install required libraries and the dependencies

```
pip install --upgrade pip
pip install pandas openpyxl xlsxwriter pyinstaller
```

### 5. Edit Input and Output paths in ```config.yaml```
```
paths:
  input_folder: {INPUT FOLDER PATH}
  output_folder: {OUTPUT FOLDER PATH}
  temp_data: {TEMP DATA CSV}
```


### 6. Compile Python file into .exe
#### Linux / macOS
```
pyinstaller --onefile --name MainApp \
  --hidden-import=openpyxl \
  --hidden-import=xlsxwriter \
  main.py
```
#### Windows (PowerShell or CMD)
```
pyinstaller --onefile --name MainApp `
  --hidden-import openpyxl `
  --hidden-import xlsxwriter `
  main.py

```
The compiled .exe will be generated inside the `dist/` folder

### 7. Build Clean up
#### Linux / macOS
```
mv dist/MainApp ./MainApp
rm -rf build dist MainApp.spec
```
#### Windows (PowerShell)
```
Move-Item dist\MainApp.exe .\MainApp.exe -Force
Remove-Item -Recurse -Force build, dist
Remove-Item -Force MainApp.spec
```
#### Windows (CMD)
```
move dist\MainApp.exe MainApp.exe
rmdir /s /q build
rmdir /s /q dist
del MainApp.spec
```

### 📂 Project Structure (after compilation and moving)
```
mainfolder/
│── input-folder/    
│── output/
│── main.py
│── temp_data.csv          # Temporary CSV for integrating all data in one folder
│── config.yaml            # Paths config
│── shipenv/               # (Optional) Python environment
└── MainApp.exe            # Final executable output
```

### 8. Run
#### Linux / macOS
```
./MainApp
```
#### Windows (PowerShell or CMD)
```
.\MainApp.exe
```





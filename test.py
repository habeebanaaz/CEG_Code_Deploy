import os

def filter_files(directory):
    # List to hold filtered file names
    filtered_files = []
    
    # Loop through each file in the specified directory
    for filename in os.listdir(directory):
        # Check if the file starts with "DeliveryPlan" and does NOT end with "Exelon"
        if filename.startswith("DeliveryPlan") and not filename.__contains__("Exelon"):
            filtered_files.append(filename)
    
    return filtered_files

# Specify the directory to filter files
directory_path = "//cgc_nt21/Vol1/common/GAS/CONTROL/Delivery Plans/FY2024\Oct2023"
filtered = filter_files(directory_path)

# Print the filtered list of files
print(filtered)
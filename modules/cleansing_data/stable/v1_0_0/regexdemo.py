import re
import datetime

x = datetime.datetime.now()
print(x)

file_path = "log/pipeline.log"
with open(file_path, "r") as file:
    data = file.read()
    

pattern_reg = r"(20\d{2}-(0[1-9]|1[1,2])-(0[1-9]|[12][0-9]|3[01]))\s+((?:[01]?[0-9]|2[0-3]):[0-5]?[0-9]:[0-5]?[0-9]),\d{3}:Classdemo:20:export_file:Created"
Date_log_regex =re.search(pattern_reg,data)
if Date_log_regex :
    print("Found")
    Hidden_count = 1
else : 
    print("404 Not Found")
    Hidden_count = 0

# Define pattern_reg in the same cell before using it
# The original pattern_reg was causing a SyntaxError due to an unterminated string literal.
# The & and ^ characters have been removed, and the pattern has been adjusted to
# capture date and time separately using capturing groups.
pattern_reg = r"(20\d{2}-(0[1-9]|1[1,2])-(0[1-9]|[12][0-9]|3[01]))\s+((?:[01]?[0-9]|2[0-3]):[0-5]?[0-9]:[0-5]?[0-9]),\d{3}:Classdemo:20:export_file:Created"

match = re.findall(pattern_reg, data)

# Process the output to keep only the date and time
# Change item[3] to item[1] to access the second element of the tuple (time)
processed_match = [(item[0], item[3]) for item in match] # Corrected the index to 1 to get the time string
unique_matches = list(set(processed_match))

for match in unique_matches:
    date = match[0]
    time = match[1]
    # Do something with the date and time
    print(f"Date: {date}, Time: {time}")

# 2. Convert times to datetime objects for calculations:
datetime_objects = []
for date_time_tuple in unique_matches: # Iterate through the list of tuples
    date_time_str = f"{date_time_tuple[0]} {date_time_tuple[1]}"
    datetime_object = datetime.datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")
    datetime_objects.append(datetime_object)
    
print("-------------------------------------------------------------------------------------------")
today = datetime.datetime.now()
today_time = today.strftime("%Y-%m-%d %H:%M:%S") # Format the current time
print(f"Now is: {today_time}")


# Calculate the difference between today and the first datetime object
for time_diff in datetime_objects:
    diff = today - time_diff
    # Now diff is a timedelta object, which you can use to get the difference in various units
    diff_months = today.month - time_diff.month
    diff_years = today.year - time_diff.year

    print(f"This file created at {time_diff}")
    print(f"Day diff from now is {diff}")  # Output the total difference
    print(f"Day diff year is {diff_years} years")
    print(f"Day diff month is {diff_months} month")
    print(f"Day unit diff is {diff.days} days")  # Output the difference in days
    print(f"Hours unit diff is {diff.seconds // 3600} hrs.")  # Output the difference in hours
    print(f"minutes unit diff is {(diff.seconds % 3600) // 60} mins.")  # Output the difference in minutes
    print("-----------------------------------------------------------------")
    
All_file = len(datetime_objects)
print(f"The total file has {All_file} files")

count = 0  # Initialize count as an integer
#from itertools import count  # Remove or comment out this line

for counter in datetime_objects:
    diff = today - counter  # Calculate diff inside the loop for each datetime object
    if diff.days > 1:
        count = count + 1  # Increment the integer count
    else:
        count


print(f"Found the file that lifetime more than one day have {count} files")  # Print the final count value, not the string "count"


# Define the regex pattern
pattern = r"Warehouse/Unhash/\d{2}\.csv"  # Escape the dot (.) as it's a special character

# Find all occurrences
matches_reg  = re.findall(pattern, data)

# Print the matches
print(matches_reg)
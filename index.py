import sys
import pandas as pd
from sqlalchemy import create_engine
import math, time, os, sys, re
from openpyxl import load_workbook
from dotenv import load_dotenv

# from reportlab.pdfbase.ttfonts import TTFont





class DBConnect:
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = (password)
    
    def exit_program(self):
        print("Exiting the program...")
        sys.exit(0)
        

    def connect_to_db(self):
        try:
            host_port = f"{self.server},1433"
            name = self.database
            print(f"\nAttempting to connect to:")
            print(f"Server: {self.server}")
            print(f"Database: {self.database}")
            
            # Modified connection string to match Django's style
            url = conn_str = (
                f"mssql+pyodbc://{self.username}:{self.password}@{self.server}/{self.database}"
                "?driver=ODBC+Driver+17+for+SQL+Server"
            )
            engine = create_engine(
                url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
                connect_args={"timeout": 10},
                fast_executemany=True
            )
            
            con = engine.connect()
            if con:
                print(f"\nSuccessfully connected to {self.database} on {self.server}")
            return con

        except Exception as e:
            print(f'\nConnection Error: {str(e)}')
            print('\nPlease verify these match your Django settings:')
            print(f'1. Server: {self.server}')
            print(f'2. Database: {self.database}')
            print(f'3. Username: {self.username}')
            print('4. ODBC Driver 17 is installed')
            self.exit_program()

class FetchData:
    def __init__(self, db_connect, start_date, end_date, sql):
        self.connect = db_connect #instance of the class DBConnect where exit program is stored
        self.start_date = start_date
        self.end_date = end_date
        self.sql_file = sql

     

    def fetchdata(self):
            con = self.connect.connect_to_db()
            try:
            # Get the maximum CreatedAt date from the Customers table
                end = pd.read_sql("SELECT MAX(SERVICEEND)  max_service_end FROM HIS_PFRECORD", con)
                latest_created_at = end['max_service_end'].iloc[0]

                #min date
                start = pd.read_sql('Select min(SERVICESTART) As min_service_start FROM HIS_PFRECORD', con)
                oldest_created_at = start['min_service_start'].iloc[0]


                # Check if the provided end_date is greater than the max date in the database
                if self.end_date > latest_created_at and self.start_date < oldest_created_at:
                    print(f"Data available from {oldest_created_at.date()} to {latest_created_at.date()} only.")
                    self.connect.exit_program()
                    return
                
                elif self.end_date > latest_created_at:
                    print(f"Data available on {latest_created_at.date()} only.")
                    self.connect.exit_program()
                    return 

                elif self.start_date < oldest_created_at:
                    print(f"Data available from {oldest_created_at.date()} only.")
                    self.connect.exit_program()
                    return


                with open(self.sql_file, 'r') as file:
                    sql_query = file.read()

                

                df = pd.read_sql(sql_query, con, params=(self.start_date, self.end_date))

                df1 = df[df['ADMISSIONTYPE'] == 'IPD']
                df2 = df[df['ADMISSIONTYPE'] == 'OPD']            
               
                return df1, df2
            
            
            
            except ValueError:
                print('Invalid input: Please enter a valid date: YYYY-MM-DD.')
                self.connect.exit_program()
                return
        
            finally:
                if 'con' in locals() and con:
                    con.close()


class CleanDF:
    def __init__(self):
        pass

    def format_and_apply(self, df, cols):
        for col in cols:
            df[col] = df[col].fillna(0).map('{:,}'.format).round(2) 
        return df
    
    def clean_string(self, df, cols):
        for col in cols:
            if col in cols:
                df[col] = df[col].fillna('').str.replace(r'\s+', ' ', regex=True).str.strip()
            else:
                print(f"Warning: Column '{col}' not found in DataFrame.")
        return df
 
       
class DataFrame:
    def __init__(self, df):
        self.df = df
        self.format = CleanDF()  
    
    def apply_format(self):
        columns_to_format = ['GROSS BILLED TO DOCTOR', 'WH @ Source', '(NET) BILLED TO DOCTOR', 'TOTAL PAYMENTS', 'PF Paid (WH)', 'BILLED TO DOCTOR']
    
        self.df = self.format.format_and_apply(self.df, columns_to_format) 
        print(self.df.info())
        return self.df
    
    def clean_string(self):
        col =['PHIC_RECEIPT', 'COMP_RECEIPT', 'PER_RECEIPT']
        self.df = self.format.clean_string(self.df, col)
        return self.df



class Summation:
    def __init__(self, df1, df2):
        self.df1 = df1
        self.df2= df2
        # self.dataFrame = DataFrame(df)
        # self.cleanData = CleanDF()

    

    def get_sum(self):
        df = self.df.copy()

        

        col_sum = ['GROSS', 'DISCOUNT', 'ARPHIC', 'ARPHIC (WH)', 'ACTUAL PHIC',
                'PHIC_PAID', 'PHIC_WH TAX Source', 'PHIC_ACTUAL PAID']
        
        grouped_df = df.groupby('DISCHARGED')[col_sum].sum().reset_index()


        grouped_df['DISCHARGED'] = grouped_df['DISCHARGED'].astype(str)

        result_df = pd.DataFrame() 
        
        for date in df['DISCHARGED'].unique():
            # Filter rows for the current date
            temp_df = df[df['DISCHARGED'] == date].copy()

            # Get the sum row for the current date
            sum_row = grouped_df[grouped_df['DISCHARGED'] == str(date)].copy()

            # Assign 'Total' to the 'DISCHARGED' column
            sum_row.loc[:, 'DISCHARGED'] = 'Total'

            # Append the original rows, the total row, and two empty rows
            result_df = pd.concat([result_df, temp_df, sum_row], ignore_index=True)

            # Add two empty rows
            empty_rows = pd.DataFrame([[''] * len(df.columns)] * 2, columns=df.columns)
            result_df = pd.concat([result_df, empty_rows], ignore_index=True)
        
        return result_df


class CleanDF:
    def __init__(self):
        pass
 


    def remove_col(self, df1, df2):
        col = [ 'ADMISSIONTYPE', 'ADMITTED', 'LASTNAME']
        df1 = df1.drop(columns=[c for c in col if c in df1.columns], errors='ignore')
        df2 = df2.drop(columns=[c for c in col if c in df2.columns], errors='ignore')

        return df1, df2 



    def summ(self, df1, df2):
        def insert_sums(df):
            col_excluded = {'HOSPRECNO', 'DISCHARGED', 'CONFINEMENT', 'PATIENT'} #exclude from summation
            grouped = df.groupby('DISCHARGED')

            result_list = []
            
            #sumtotal

            # Collect grand total
            grand_total = df.iloc[:1].copy()
            grand_total = grand_total.astype(object)
            grand_total.iloc[:, :] = ''
            grand_total['HOSPRECNO'] = 'GRAND TOTAL'

            for col in df.columns:
                if col not in col_excluded:
                    grand_total[col] = df[col].sum().round(2)
         

            
            for name, group in grouped:
                # create empty row
                empty_row = pd.DataFrame({col: [''] for col in df.columns})

                sum_row = group.iloc[:1].copy()  # Copy first row structure
                sum_row = sum_row.astype(object)  # convert datatype to object
                sum_row.iloc[:, :] = ''  # assign empty string 
                
                
                # sum relevant columns
                for col in df.columns:
                    if col not in col_excluded:
                        sum_row[col] = group[col].sum().round(2)
                
                sum_row['DISCHARGED'] = name  
                sum_row['HOSPRECNO'] = 'TOTAL' #

                # append grouped data, empty row and sum row
                result_list.append(group)
                result_list.append(empty_row)  
                result_list.append(sum_row)
                result_list.append(empty_row)

            result_list.append(grand_total)

            return pd.concat(result_list, ignore_index=True)

      



        df1 = insert_sums(df1).drop(columns=['DISCHARGED'])
        df2 = insert_sums(df2).drop(columns=['DISCHARGED'])


        return df1, df2
    

    def add_code(self, df1, df2):
        def insert_code(df):
            abss_code = {
                "DISCOUNT": 92100,
                "ARPHIC": 15111,
                "ARCOMP": 15131,
                "ARHMO": 15141,
                "ARPERSONAL": 15121,
                "ICU": 41100,
                "MEDICINE": 45100,
                "ROOM AND BOARD": 41100,
                "OR CHARGES": 41200,
                "NICU": 41400,
                "CENTRAL SUPPLIES": 44700,
                "ER CHARGES": 43100,
                "LABORATORY": 42300,
                "DELIVERY ROOM": 41300,
                "HEMODIALYSIS CHARGES": 42700,
                "MISCELLANEOUS CHARGES": 44700,
                "ENDOSCOPY": 47300,
                "DIETARY": 81000,
                "LINEN": 44700,
                "HEART CENTER CHARGES": 44500,
                "EYE CENTER CHARGES": 49100,
                "NUCLEAR CHARGES": 44450,
                "ECG": 48700,
                "DISPENSARY CHARGES": 43100,
                "CHEMOTHERAPY CHARGES": 427500,
                "CTSCAN": 44300,
                "MAMMOGRAM": 44400,
                "MRI": 44350,
                "ULTRASOUND": 44200,
                "XRAY": 44100
            }

            column_tuples = []
            for col in df.columns:
                col_upper = col.upper().strip()

                # Exact match first
                code = abss_code.get(col_upper, None)
                
                # match 
                if code is None:
                    for key in abss_code:
                        if f" {key} " in f" {col_upper} ":
                            code = abss_code[key]
                            break

                # when no match found
                if code is None:
                    code = ' '

                column_tuples.append((code, col))

            df.columns = pd.MultiIndex.from_tuples(column_tuples)
            return df

        df1 = insert_code(df1)
        df2 = insert_code(df2)

        return df1, df2






def main():
    load_dotenv()
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')

    db_connect = DBConnect(server, database, username, password)
    # Get start and end dates from the user
    start_date = input("Start date (YYYY-MM-DD): ")
    end_date = input("End date (YYYY-MM-DD): ")

    # Convert the date input to datetime format
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    while True:
        try:
            name = int(input("\nSelect name: [1]Ma'am Ann  [2]Sir Dean: "))
            if name in [1, 2]:
                break
            else:
                print("Invalid selection. Please enter 1 or 2.\n\n")
        except ValueError:
            print("Invalid input. Please enter a number.\n\n")

    sql1 = 'query.sql'
    sql2 = 'queryD.sql'

    if name == 1:
        fetch_data = FetchData(db_connect, start_date, end_date, sql1)
        df1, df2 = fetch_data.fetchdata()

        cleandf = CleanDF()
        df1, df2 = cleandf.remove_col(df1, df2)
        df1, df2 = cleandf.summ(df1, df2)
        df1, df2 = cleandf.add_code(df1, df2)
    else:
        fetch_data = FetchData(db_connect, start_date, end_date, sql2)
        df1, df2 = fetch_data.fetchdata()



    def increment_filename(base_name, extension):
        """Generate an incremented file name."""
        counter = 1
        while True:
            file_path = f"{base_name}{counter}.{extension}"
            if not os.path.exists(file_path):
                return file_path
            counter += 1

    file_path = increment_filename("2_task", "xlsx")

    while True:
        try:
            key = int(input("\n[1]IPD  [2]OPD  [3]Both: "))
            if key in [1, 2, 3]:
                print(f'File "{file_path}" saved to "{os.getcwd()}".')
                break
            else:
                print("Invalid selection. Please choose a valid option.\n")
        except ValueError:
            print("Invalid input. Please enter a number.\n")

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        if key == 1:
            df1.to_excel(writer, sheet_name="IPD", index=True)
        elif key == 2:
            df2.to_excel(writer, sheet_name="OPD", index=True)
        elif key == 3:
            df1.to_excel(writer, sheet_name="IPD", index=True)
            df2.to_excel(writer, sheet_name="OPD", index=True)



if __name__ == "__main__":
    main()
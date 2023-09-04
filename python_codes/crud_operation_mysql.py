import mysql.connector as mysql
from pandas import DataFrame
from tabulate import tabulate


class MysqlDBConnection :
    menu_msg = " \
    ==========================================================================\n \
    c - Create \n \
    i - Insert \n \
    u - Update\n \
    d - Drop \n \
    v - Display \n \
    t - Truncate \n \
    q - Quit\n \
    ============================================================================="

    def __init__(self) :
        self.cnx = mysql.connect(host="localhost",
                                 user="root",
                                 password="root",
                                 database="test_db")
        self.cursor = self.cnx.cursor()

    def process_user_options(self) :
        inp = ''
        while inp != 'q' :
            print(self.menu_msg)
            usr_input = input("Please enter your option :\n").split()
            if not usr_input :
                continue  # no option typed
            elif usr_input[0] == 'q' :
                inp = [0]
                print("Process completed")
                break
            else :
                return usr_input

    def create_mysql_table(self):
        # Create table operation started
        print("\nTable list before table creation\n")
        show_table_query = "show tables"
        self.cursor.execute(show_table_query)
        for row in self.cursor :
            print(row)

        # Get the table name and columns details for table creation
        table_name = input("\nEnter the table name to create: ")
        column_count = int(input("Enter the number of columns to create: "))
        column_name = []
        column_type = []
        for c_count in range(0, column_count) :
            element = (input(f"Enter the column name{[c_count]}:"))
            column_name.append(element)
            col_type = (input(f"Enter the column type{[c_count]}:"))
            column_type.append(col_type)
        column_detail_structure = ""
        for col in range(0, column_count) :
            column_detail_structure += f"{column_name[col]} {column_type[col]},"

        # Create table query for MySQL operations
        create_sql_query = f"create table if not exists {table_name} ({column_detail_structure[:-1]})"
        print("\ncreate_table_query\n", create_sql_query)
        self.cursor.execute(create_sql_query)
        print("\nTable list before table creation\n")
        show_table_query = "show tables"
        self.cursor.execute(show_table_query)
        for row in self.cursor :
            print(row)

    def insert_mysql_table(self):
        # Insertion operation started
        print("Insertion operation started")

        # Schema name is needed to get the column names
        # Table name is needed
        schema_name = input("\nEnter the schema name: ")
        table_name = input("\nEnter the table name to insert: ")
        print(f"schema_name : {schema_name}, table_name : {table_name}")

        # After got the table name, need to find columns and column count
        # Below query gave the count of columns
        column_count_query = f"SELECT count(column_name) FROM information_schema.columns WHERE table_schema ='{schema_name}' and table_name='{table_name}'"
        self.cursor.execute(column_count_query)
        column_count_result = self.cursor.fetchone()
        print(
            f"Table {table_name} had {column_count_result[0]} columns. So need to add {column_count_result[0]} column data for each record")
        # Below Describe query to get the column name for insertion
        get_column_names_query = f"desc {table_name}"
        self.cursor.execute(get_column_names_query)
        column_list = self.cursor.fetchall()
        details_to_send = []
        for column in column_list :
            one_columns_list = column[0]
            details_to_send.append(one_columns_list)
        # Here details_to_send is column_list
        print("Column_list", details_to_send)

        # For insertion operation, arrange the column names according to insert query sql syntax
        column_names_for_insert_query = ""
        for col_index in range(0, column_count_result[0]) :
            column_names_for_insert_query += f"{details_to_send[col_index]},"

        # For insertion operation, get the column data from console and arrange them according to insert query sql syntax
        records_list = []
        record_count = int(input("Enter how many records you want to insert: "))
        for col_index in range(0, record_count):
            record_data = input(f"Enter the record{[col_index]}: ")
            records_list.append(record_data)
        print(records_list)
        column_data_for_insert_query = ""
        for col_index in range(0, record_count):
            column_data_for_insert_query += f"{records_list[col_index]},"

        # Insert statement query for MySQL database
        insert_query = f"insert into {table_name}({column_names_for_insert_query[:-1]}) values{column_data_for_insert_query[:-1]}"
        print(insert_query)
        self.cursor.execute(insert_query)
        # Commit statement used for commit our changes to original mysql table, otherwise it stays on buffer
        self.cursor.execute("commit")

        # Insertion operation started
        print("Insertion operation ended")

    def truncate_table_operation(self):
        # Truncate table operation start
        print("Truncate table operation started")
        table_name = input("Enter the table name to truncate: ")

        # Truncate table query for MySQL operation
        truncate_table_query = f"truncate table {table_name}"
        print(truncate_table_query)
        self.cursor.execute(truncate_table_query)

        # Truncate table operation start download_to_local_folder
        print("Truncate table operation ended")

    def update_table_operation(self):

        # Before update, we don't know the table data, so we call display table operation first
        print("Before update, we don't know the table data, so we need to view table operation first")
        self.display_table_operation()

        # Update table operation start
        print("\nUpdate table operation started")
        table_name = input("Enter the table name to update: ")

        # After got the table name, need to find columns
        # Below Describe query to get the column name for insertion
        get_column_names_query = f"desc {table_name}"
        self.cursor.execute(get_column_names_query)
        column_list = self.cursor.fetchall()
        details_to_send = []
        for column in column_list :
            one_columns_list = column[0]
            details_to_send.append(one_columns_list)
        # Here details_to_send is column_list
        print("Column_list", details_to_send)

        # Get the count of columns need to be edited
        column_edit_count = int(input("Enter how many columns to edit : "))
        print(column_edit_count)

        # Get the edited column name choose by the user
        column_name_edit_list = []
        for col_index in range(0, column_edit_count):
            update_column_name = input(f"Enter the column_name[{col_index}] need to edit : ")
            column_name_edit_list.append(update_column_name)
        print(column_name_edit_list)

        # Get the edited column value for corresponding column names
        column_value_list = []
        for col_index in range(0, column_edit_count):
            update_column_value = input(f"Enter the edited value for column '{column_name_edit_list[col_index]}' : ")
            column_value_list.append(update_column_value)

        print(column_value_list)

        # For update operation, arrange the column edit changes according to insert query sql syntax
        edit_columns_make_query = ""
        for col_index in range(0, column_edit_count):
            edit_columns_make_query += f"{column_name_edit_list[col_index]} = {column_value_list[col_index]},"

        print(edit_columns_make_query)

        # After got edit column names and values we need to get condition for update table sql query
        update_query_condition_column = input("Enter the conditioned column: ")
        update_query_condition_value = input(f"Enter the condition value for column {update_query_condition_column} : ")

        update_table_query = f"update {table_name} set {edit_columns_make_query[:-1]} " \
                             f"where {update_query_condition_column} = {update_query_condition_value}"
        print(update_table_query)
        self.cursor.execute(update_table_query)

        # Commit statement used for commit our changes to original mysql table, otherwise it stays on buffer
        self.cursor.execute("commit")

        # Insertion operation started
        print("Update operation ended")

    def drop_table_operation(self):
        # Drop table operation start
        print("Drop table operation started")
        table_name = input("Enter the table name to drop: ")

        # Truncate table query for MySQL operation
        drop_table_query = f"drop table {table_name}"
        print(drop_table_query)
        self.cursor.execute(drop_table_query)

        # Truncate table operation end
        print("Drop table operation ended")

    def display_table_operation(self) :
        # Display table operation start
        print("Select table operation started")
        table_name = input("Enter the table name to display: ")

        # Display table query for MySQL operation
        select_table_query = f"select * from {table_name}"
        print(select_table_query)
        self.cursor.execute(select_table_query)
        result_set = DataFrame(self.cursor.fetchall())
        result_set.columns = self.cursor.column_names

        print(result_set.columns)

        # pip install tabulate.tabulate()
        # print(result_set.to_markdown())
        print(tabulate(result_set, headers=result_set.columns, tablefmt='psql'))

        # Display table operation end
        print("Display table operation ended")

    def run_user_cmd(self, usr_input):
        if usr_input[0] == 'q':
            exit(0)
        elif usr_input[0] == "c":
            print("Create record in the table")
            self.create_mysql_table()
        elif usr_input[0] == 'i':
            print("Insert record in the table")
            self.insert_mysql_table()
        elif usr_input[0] == 'u':
            print("Update record in the table")
            self.update_table_operation()
        elif usr_input[0] == 'd':
            print("Drop record in the table")
            self.drop_table_operation()
        elif usr_input[0] == 'v':
            print("Display record in the table")
            self.display_table_operation()
        elif usr_input[0] == 't':
            print("Truncate table")
            self.truncate_table_operation()
        else :
            print("\n\n ** ERROR ** enter only valid input")


def main() :
    mysql_helper = MysqlDBConnection()
    user_input = mysql_helper.process_user_options()
    mysql_helper.run_user_cmd(user_input)


if __name__ == "__main__" :
    main()

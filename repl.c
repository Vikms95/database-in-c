// *pointer_address->value = 123 - modify value of where the pointer is pointing to
// &(pointer_address) -  reference the pointer address in memory, not where the pointer points to
// pointer_address->value = NULL - usually always used with NULL, since it modifies where the pointer is pointing, the pointer address
// and it is weird you would do pointer->value = 0x38287832

// creating memory for variable and pointer is done with "malloc"
// freeing memory for variable and pointer id done with free(pointer_address)

#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100

typedef enum
{
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef enum
{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
} MetaCommandResult;

typedef enum
{
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT,
} PrepareResult;

typedef enum
{
    EXECUTE_TABLE_FULL,
    EXECUTE_SUCCESS,
} ExecuteResult;

typedef struct
{
    char *buffer;         // Pointer
    size_t buffer_length; // Unsigned integer type (only can be positive)
    ssize_t input_length; // Signed integer which can represent errors with the value -1
} InputBuffer;

typedef struct
{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} Row;

typedef struct
{
    StatementType type;
    Row row_to_insert;
} Statement;

typedef struct
{
    uint32_t num_rows;
    void *pages[TABLE_MAX_PAGES];
} Table;

// This macro calculates the size of a specific attribute within a structure.
// It uses the sizeof operator to determine the size in bytes of the attribute of a struct.
// The syntax ((Struct*) 0)->Attribute is a common C idiom for obtaining the offset of a field
//  within a struct without actually having a valid instance of the struct.
// Here, it's being used to get the size of the field.
#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)
// This constant holds the size of the id field within the Row struct, measured in bytes.
// Since id is of type uint32_t within the Row struct, ID_SIZE will typically be 4 bytes
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

/* Compute the memory address of a row within a table. We return a pointer
to an undetermined data type (void*) */
void *row_slot(Table *table, uint32_t row_num)
{
    // Determine in which page the row is located
    // row_num = 203
    // rows_per_page = 100
    // page_num -> 203 / 100 = 2
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = table->pages[page_num];
    if (page == NULL)
    {
        // Allocate memory only when we try to acces tge page
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    // Determine the row position relative to the start of the page
    // row_num = 203
    // rows_per_page = 100
    // row_offset -> 203 % 100 = 3 (4th row since this is 0 indexed)
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

InputBuffer *new_input_buffer()
{
    // Create input_buffer and allocate memory based on the size of the members
    // defined in the struct, which the C compiler calculates on runtime
    InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
    // Reset pointer address from input_buffer
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

Table *new_table()
{
    // Cast the pointer returned by malloc to a Table type instead of an unknown one
    Table *table = (Table *)malloc(sizeof(Table));
    table->num_rows = 0;
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        table->pages[i] = NULL;
    }
    return table;
}

MetaCommandResult do_meta_command(InputBuffer *input_buffer)
{
    if (strcmp(input_buffer->buffer, ".exit") == 0)
    {
        exit(EXIT_SUCCESS);
    }
    else
    {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement)
{
    /* tries to match 6 characters of the buffer, since insert will be followed
     by more data afterwards the keyword*/
    if (strncmp(input_buffer->buffer, "insert", 6) == 0)
    {
        statement->type = STATEMENT_INSERT;
        int args_assigned = sscanf(input_buffer->buffer, "insert %d %s %s", &(statement->row_to_insert.id), statement->row_to_insert.username, statement->row_to_insert.email);
        if (args_assigned < 3)
        {
            return PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }
    if (strcmp(input_buffer->buffer, "select") == 0)
    {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement *statement, Table *table)
{
    // If the row cap for the table is reached, do not insert
    if (table->num_rows >= TABLE_MAX_ROWS)
    {
        return EXECUTE_TABLE_FULL;
    }

    Row *row_to_insert = &(statement->row_to_insert);

    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    table->num_rows += 1;
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement, Table *table)
{
    Row row;
    for (uint32_t i = 0; i < table->num_rows; i++)
    {
        {
            deserialize_row(row_slot(table, i), &row);
            print_row(&row);
        }
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *statement, Table *table)
{
    switch (statement->type)
    {
    case (STATEMENT_INSERT):
        return execute_insert(statement, table);
    case (STATEMENT_SELECT):
        return execute_select(statement, table);
    }
}

void read_input(InputBuffer *input_buffer)
{
    ssize_t bytes_read =
        getline(
            &(input_buffer->buffer),        // Pointer to buffer where the read line will be stored
            &(input_buffer->buffer_length), // Pointer to variable that hold the size of the buffer
            stdin                           // The input stream to read from (cli stdin)
        );

    // If no bytes read, exit program with failure
    if (bytes_read <= 0)
    {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // Assign number of bytes read to input_length and ignore trailing newline (\n)(substract 1)
    int buffer_to_assign = bytes_read - 1;
    input_buffer->input_length = buffer_to_assign;
    input_buffer->buffer[buffer_to_assign] = 0;
}

void close_input_buffer(InputBuffer *input_buffer)
{
    free(input_buffer->buffer);
    free(input_buffer);
}

/* Free the memory allocated for each table page to finally free the table itself*/
void free_table(Table *table)
{
    for (uint32_t i = 0; table->pages[i]; i++)
    {
        free(table->pages[i]);
    }
    free(table);
}

void print_prompt() { printf("db > "); }

void print_row(Row *row)
{
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void serialize_row(Row *source, void *destination)
{
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination)
{
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

int main(int argc, char *argv[])
{
    Table *table = new_table();

    InputBuffer *input_buffer = new_input_buffer();
    while (true)
    {
        print_prompt();
        read_input(input_buffer);
        // When writing ".exit" on terminal (strmpc compares and if equal returns 0), the program exits
        if (strcmp(input_buffer->buffer, ".exit") == 0)
        {
            close_input_buffer(input_buffer);
            exit(EXIT_SUCCESS);
        }
        else if (input_buffer->buffer[0] == '.')
        {
            switch (do_meta_command(input_buffer))
            {
            case META_COMMAND_SUCCESS:
                continue;
            case META_COMMAND_UNRECOGNIZED_COMMAND:
                printf("Unrecognized command '%s'\n", input_buffer->buffer);
                continue;
            }
        }

        Statement statement;
        PrepareResult prepare_result = prepare_statement(input_buffer, &statement);
        switch (prepare_result)
        {
        case (PREPARE_SUCCESS):
            break;
        case (PREPARE_SYNTAX_ERROR):
            printf("Syntax error. Could not parse statement.\n");
            continue;
        case (PREPARE_UNRECOGNIZED_STATEMENT):
            printf("Unrecognized keyword at start of '%s'.\n",
                   input_buffer->buffer);
            continue;
        }

        ExecuteResult execute_result = execute_statement(&statement, table);
        switch (execute_result)
        {
        case (EXECUTE_SUCCESS):
            printf("Executed. \n");
            break;
        case (EXECUTE_TABLE_FULL):
            printf("Error table full. \n");
            break;
        }
    }
}

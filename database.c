// PART 5 - changed Table struct, now fix errors// *pointer_address->value = 123 - modify value of where the pointer is pointing to
// &(pointer_address) -  reference the pointer address in memory, not where the pointer points to
// pointer_address->value = NULL - usually always used with NULL, since it modifies where the pointer is pointing, the pointer address
// and it is weird you would do pointer->value = 0x38287832

// QUESTION:
// do not understand why sometimes values (like Statement on main function) are being passed as &statement as sometimes without &
// difference between variable->value.id and variable.value or variable->value.value

// NEXT: Allocating New Pages

#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

// Constants to define maximum sizes for username and email fields.
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
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID,
    PREPARE_UNRECOGNIZED_STATEMENT,
} PrepareResult;

typedef enum
{
    EXECUTE_TABLE_FULL,
    EXECUTE_SUCCESS,
    EXECUTE_DUPLICATE_KEY,
} ExecuteResult;

// Leaf nodes and internal nodes have different sizes, differentiate them.
typedef enum
{
    NODE_INTERNAL,
    NODE_LEAF
} NodeType;
// Struct to manage pages of data stored in a file.
typedef struct
{
    // Value from opening the file
    int file_descriptor;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];
    uint32_t num_pages;
} Pager;

// Struct for managing user input.
typedef struct
{
    char *buffer;         // Pointer
    size_t buffer_length; // Unsigned integer type (only can be positive)
    ssize_t input_length; // Signed integer which can represent errors with the value -1
    uint32_t root_page_num;
} InputBuffer;

// Struct representing a single row in the database.
typedef struct
{
    uint32_t id;
    // Allocating an extra character since all C strings should be ended with a NULL character
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

// Struct for a database statement.
typedef struct
{
    StatementType type;
    Row row_to_insert;
} Statement;

// Struct representing the database table.
typedef struct
{
    uint32_t num_rows;
    Pager *pager; // Refence to the general pager. This is done to avoid passing down the pager as parameters.
    uint32_t root_page_num;
} Table;

typedef struct
{
    Table *table;      // Reference to the table its part of. This is done to avoid passing down the table as parameters.
    bool end_of_table; // Indicates a position one past the last element
    uint32_t page_num;
    uint32_t cell_num;
} Cursor;

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

// Node header format
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = sizeof(uint8_t);
const uint32_t PARENT_POINTER_SIZE = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Leaf node format
// In addition to these common header fields, leaf nodes need to store how many “cells” they contain. A cell is a key/value pair.

// Lead node header layout
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// Leaf node body layout
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

// Internal node header
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE * INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

// Internal node body
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_RIGHT_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

void debug_table(Table *table)
{
    printf("Cursor is being debugged.\n");
    if (table == NULL)
    {
        printf("Table is NULL.\n");
        return;
    }
    printf("Table info:\n");
    printf("  num_rows: %u\n", table->num_rows);
    printf("  root_page_num: %u\n", table->root_page_num);

    if (table->pager == NULL)
    {
        printf("  Pager is NULL.\n");
        return;
    }

    printf("  Pager info:\n");
    printf("    file_descriptor: %d\n", table->pager->file_descriptor);
    printf("    file_length: %u\n", table->pager->file_length);
    printf("    num_pages: %u\n", table->pager->num_pages);
}
void debug_cursor(Cursor *cursor)
{
    printf("Cursor is being debugged.\n");
    if (cursor == NULL)
    {
        printf("Cursor is NULL.\n");
        return;
    }
    printf("Cursor info:\n");
    printf("  page_num: %u\n", cursor->page_num);
    printf("  cell_num: %u\n", cursor->cell_num);

    if (cursor->table == NULL)
    {
        printf("  Table is NULL.\n");
        return;
    }

    if (cursor->table->pager == NULL)
    {
        printf("  Pager is NULL.\n");
        return;
    }

    printf("  Pager info:\n");
    printf("    file_descriptor: %d\n", cursor->table->pager->file_descriptor);
    printf("    file_length: %u\n", cursor->table->pager->file_length);
    printf("    num_pages: %u\n", cursor->table->pager->num_pages);
}

uint32_t *internal_node_num_keys(void *node)
{
    return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t *internal_node_right_child(void *node)
{
    return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t get_unused_page_num(Pager *pager)
{
    return pager->num_pages;
}

uint32_t *internal_node_cell(void *node, uint32_t cell_num)
{
    return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

uint32_t *internal_node_child(void *node, uint32_t child_num)
{
    uint32_t num_keys = *internal_node_num_keys(node);
    if (child_num > num_keys)
    {
        printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
        exit(EXIT_FAILURE);
    }
    else if (child_num == num_keys)
    {
        return internal_node_right_child(node);
    }
    else
    {
        return internal_node_cell(node, child_num);
    }
}

uint32_t *internal_node_key(void *node, uint32_t key_num)
{
    return internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

uint32_t get_node_max_key(void *node)
{
    switch (get_node_type(node))
    {
    case NODE_INTERNAL:
        return *internal_node_key(node, *internal_node_num_keys(node) - 1);
    case NODE_LEAF:
        return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
    }
}

bool is_node_root(void *node)
{
    uint8_t value = *((uint8_t *)(node + IS_ROOT_OFFSET));
    return (bool)value;
}

bool set_node_root(void *node, bool is_root)
{
    uint8_t value = is_root;
    *((uint8_t *)(node + IS_ROOT_OFFSET)) = value;
}

void create_new_root(Table *table, uint32_t right_child_page_num)
{
    void *root = get_page(table->pager, table->root_page_num);
    void *right_child = get_page(table->pager, right_child_page_num);
    uint32_t left_child_page_num = get_unused_page_num(table->pager);
    void *left_child = get_page(table->pager, left_child_page_num);

    // Left child has data copied from old root
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);

    // Root node is a new internal node with one key and two children
    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root, 0) = left_child_page_num;
    uint32_t left_child_max_key = get_node_max_key(left_child);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_num;
}

/**
 * Retrieves a pointer to the specific cell within a leaf node.
 * Cells are key/value pairs stored sequentially in the leaf node body.
 * This function calculates the start address of a specified cell based on its index.
 *
 * @param node Pointer to the start of the leaf node.
 * @param cell_num Index of the cell to access.
 * @return Pointer to the start of the specified cell.
 */
void *leaf_node_cell(void *node, uint32_t cell_num)
{
    printf("Lead node cell.\n");
    printf("node address: %p\n", node);
    printf("cell_num: %u\n", cell_num);

    // Calculate the offset
    uint32_t offset = LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
    printf("Calculated offset: %u\n", offset);

    // Calculate the cell address
    void *cell_address = (uint8_t *)node + offset; // Cast to uint8_t* for pointer arithmetic
    printf("Calculated cell address: %p\n", cell_address);
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

/**
 * Retrieves a pointer to the key part of a specific cell in a leaf node.
 * This function reuses leaf_node_cell to find the cell and then returns its starting point,
 * as the key is the first part of a cell.
 *
 * @param node Pointer to the start of the leaf node.
 * @param cell_num Index of the cell whose key is to be accessed.
 * @return Pointer to the key (uint32_t) within the specified cell.
 */
uint32_t *leaf_node_key(void *node, uint32_t cell_num)
{
    return leaf_node_cell(node, cell_num);
}
/*
 *Retrieves or creates a new page within the pager
 */
void *get_page(Pager *pager, uint32_t page_num)
{
    // If we are trying to get or allocate a number maximum to the allowed max size, error out.
    // This provides a block of memory equal to the size of one page where the data can be stored.
    if (pager->pages[page_num] == NULL)
    {
        // Allocate memory for new page
        void *page = malloc(PAGE_SIZE);
        uint32_t total_num_pages = pager->file_length / PAGE_SIZE;

        // If there is a remainder, there is an extra page being used in the file
        // but since it is not completed, we would not be counting it on the total_num_pages variable
        if (pager->file_length % PAGE_SIZE)
        {
            // This sets the file to start at the correct offset
            // (at the beggining of the desired page) for the next read operation
            off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

            if (offset == -1)
            {
                printf("Error seeking file: %d\n", errno);
                exit(EXIT_FAILURE);
            }

            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);

            if (bytes_read == -1)
            {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        // Add the created page to the pager reference
        pager->pages[page_num] = page;
        if (page_num >= pager->num_pages)
        {
            printf("get page.\n");
            pager->num_pages = +1;
        }
    }
    // Finally, the function returns the address of the page in memory, making it available for the calling function to use.
    return pager->pages[page_num];
}

NodeType get_node_type(void *node)
{
    uint32_t value = *((uint8_t *)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}
void set_node_type(void *node, NodeType type)
{
    uint8_t value = type;
    *((uint8_t *)(node + NODE_TYPE_OFFSET)) = value;
}
/**
 * Returns a pointer to the number of cells in the leaf node.
 * This function calculates the location of the cell count within the leaf node header
 * and returns a pointer to it. This allows direct modification of the cell count.
 *
 * @param node Pointer to the start of a leaf node in memory.
 * @return Pointer to the number of cells (uint32_t) in the leaf node.
 */
uint32_t *leaf_node_num_cells(void *node)
{
    return (node + LEAF_NODE_NUM_CELLS_OFFSET);
}

Cursor *leaf_node_find(Table *table, uint32_t page_num, uint32_t key)
{
    void *node = get_page(table->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = page_num;

    // Binary search
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;
    while (one_past_max_index != min_index)
    {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *leaf_node_key(node, index);
        if (key == key_at_index)
        {
            cursor->cell_num = index;
            return cursor;
        }
        if (key < key_at_index)
        {
            one_past_max_index = index;
        }
        else
        {
            min_index = index + 1;
        }
    }

    cursor->cell_num = min_index;
    return cursor;
}
/**
 * Retrieves a pointer to the value part of a specific cell in a leaf node.
 * Since values follow keys within the cell structure, this function calculates the
 * address by moving past the key portion.
 *
 * @param node Pointer to the start of the leaf node.
 * @param cell_num Index of the cell whose value is to be accessed.
 * @return Pointer to the value part within the specified cell.
 */
void *leaf_node_value(void *node, uint32_t cell_num)
{
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}
/*
 * Creates a cursor at the starting point of the table.
 */
Cursor *get_start_of_table_cursor(Table *table)
{

    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    // The boolean would be true if the table had no rows,
    // because the position 0 would be already the end of the table
    cursor->page_num = table->root_page_num;
    cursor->cell_num = 0;

    void *root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->end_of_table = (num_cells == 0);
    return cursor;
}

/*
Return the position of the given key.
If the key is not present, return the position
where it should be inserted
*/
Cursor *table_find(Table *table, uint32_t key)
{
    uint32_t root_page_num = table->root_page_num;
    void *root_node = get_page(table->pager, root_page_num);

    if (get_node_type(root_node) == NODE_LEAF)
    {
        return leaf_node_find(table, root_page_num, key);
    }
    else
    {
        printf("Need to implement searching an internal node\n");
        exit(EXIT_FAILURE);
    }
}

/*
 *Gets the offset of the page within the table and writes it back to disk to be able to persist the data and ensure durability.
 */
void pager_flush(Pager *pager, uint32_t page_num)
{
    // Although this has been checked before, double checking that the page is not NULL
    // or we could incur to a segmentation fault
    if (pager->pages[page_num] == NULL)
    {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    // Get the reference of reading the file, the number of the page times it size and
    // from the beggining of the page. We do page_num * PAGE_SIZE to skip the amount a
    // page occupies a certain amount of times.
    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if (offset == -1)
    {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    // Persist data to the file_descriptor reference
    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);

    if (bytes_written == -1)
    {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

/*
 *Unblock memory used by the page and reset it to NULL.
 */
void free_page(Pager *pager, uint32_t page_num)
{
    free(pager->pages[page_num]);
    pager->pages[page_num] = NULL;
}

/*
 *Closes the database, ensuring all data is flushed to disk and all resources are freed.
 */
void db_close(Table *table)
{
    // Access the pager reference stored within the table struct.
    Pager *pager = table->pager;
    // This only accounts for fully written pages, partially written are handled after the loop

    // Empty whatever reference the database might have
    for (uint32_t i = 0; i < pager->num_pages; i++)
    {
        if (pager->pages[i] == NULL)
        {
            continue;
        }

        pager_flush(pager, i);
        free_page(pager, i);
    }

    // Close the df opened on program startup. Not closing a file descriptor properly
    // can lead to data not being written as expected, leading to data loss or corruption or
    // surpassing the amount of file descriptors the OS can handle at once.
    // The OS would close the file descriptor if the program ends, but relying on this
    // automatic process is not recomended. There could also be unhandled error during the closing
    // process, which would not handled correctly if the OS closes the files descriptor.
    int result = close(pager->file_descriptor);
    if (result == -1)
    {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }

    // Safety net in case any pages are left unfreed via TABLE_MAX_PAGES
    // variable, which makes sure that scans all the posible pages within the table.
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        void *page = pager->pages[i];
        if (page)
        {
            free(page);
            pager->pages[i] = NULL;
        }
    }

    free(pager);
    free(table);
}
/*
 * Compute the memory address of a row within a table. We return a pointer
 * to an undetermined data type (void*)
 */
void *get_cursor_value(Cursor *cursor)
{
    // Determine in which page the row is located
    // row_num = 203
    // rows_per_page = 100
    // page_num -> 203 / 100 = 2
    uint32_t page_num = cursor->page_num;
    void *page = get_page(cursor->table->pager, page_num);
    // Determine the row position relative to the start of the page
    // row_num = 203
    // rows_per_page = 100
    // row_offset -> 203 % 100 = 3 (4th row since this is 0 indexed)
    return leaf_node_value(page, cursor->cell_num);
}

/*
 * Advance the cursor one row. If the cursor reaches the end of the table, set it as end_of_table cursor.
 */
void cursor_advance(Cursor *cursor)
{
    uint32_t page_num = cursor->page_num;
    void *node = get_page(cursor->table->pager, page_num);

    cursor->cell_num += 1;
    if (cursor->cell_num >= (*leaf_node_num_cells(node)))
    {
        cursor->end_of_table = true;
    }
}

/*
 *Creates an instance listening to the REPL input. Returns an InputBuffer struct
 */
InputBuffer *new_input_buffer()
{
    // Create input_buffer and allocate memory based on the size of the members
    // defined in the struct, which the C compiler calculates on runtime
    InputBuffer *input_buffer = malloc(sizeof(InputBuffer));

    // Reset pointer address from input_buffer. We do this since, malloc allocates a block of memory
    // and returns a pointer to it without clearing or setting its contents, which means the allocated
    // memory will contain whatever data was previously held there (often referred to as "garbage values").
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

/*
 *Creates a pager instance based on a passed filename char. Returns a Pager struct
 */
Pager *pager_open(const char *filename)
{
    // Open the file for reading and writting
    // If the file does not exist, create it
    // Seat the write and read permission to the owner of the file (whoever crated the file)
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (fd == -1)
    {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    // Determine length from position 0 till the end
    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager *pager = malloc(sizeof(Pager));

    // Value from opening the file
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = (file_length / PAGE_SIZE);

    if (file_length % PAGE_SIZE != 0)
    {
        printf("DB file is not a whole number of pages. Corrupt file.\n");
        exit(EXIT_FAILURE);
    }
    // Create as many pages as the amount of max pages a table can have
    // and initialize them to NULL
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        pager->pages[i] = NULL;
    }
    return pager;
}

/*
 *Initialized the database by instantiating a pager and a single table (for now).
 *Later give a reference of the pager to the table itself. Returns a Table struct
 */
Table *db_open(const char *filename)
{
    Pager *pager = pager_open(filename);

    Table *table = malloc(sizeof(Table));
    table->pager = pager;
    table->root_page_num = 0;

    if (pager->num_pages == 0)
    {
        // New database file. Initialize page 0 as leaf node.
        void *root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
        set_node_root(root_node, true);
    }
    return table;
}

/*
 * If the buffer started with a '.', execute one of the following meta commands. Returns
 * a value of the META_COMMAND enum to indicate the switch statement on the upper level on how to proceed.
 */
MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table)
{
    // If the value is found, 'strcmp' returns a 0.
    if (strcmp(input_buffer->buffer, ".exit") == 0)
    {
        db_close(table);
        exit(EXIT_SUCCESS);
    }
    else if (strcmp(input_buffer->buffer, ".btree") == 0)
    {
        print_tree(table->pager, 0, 0);
        return META_COMMAND_SUCCESS;
    }
    else if (strcmp(input_buffer->buffer, ".constants") == 0)
    {
        printf("Constants:\n");
        print_constants();
        return META_COMMAND_SUCCESS;
    }
    else
    {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

/*
 *Splits the insert statement . Returns a value of the prepare enum.
 */
PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement)
{
    statement->type = STATEMENT_INSERT;

    // Separate buffer on whitespace
    char *delimiter = " ";

    // Note on strtok: https://cplusplus.com/reference/cstring/strtok/
    // "strtok" maintains a static pointer within the function to keep track of where it left off in the string,
    //  allowing it to continue scanning from the end of the last token on subsequent calls.
    char *keyword = strtok(input_buffer->buffer, delimiter);
    char *id_string = strtok(NULL, delimiter);
    char *username = strtok(NULL, delimiter);
    char *email = strtok(NULL, delimiter);

    if (id_string == NULL || username == NULL || email == NULL)
    {
        return PREPARE_SYNTAX_ERROR;
    }

    if (strlen(username) > COLUMN_USERNAME_SIZE || strlen(email) > COLUMN_EMAIL_SIZE)
    {
        return PREPARE_STRING_TOO_LONG;
    }

    int id = atoi(id_string);

    if (id < 0)
    {
        return PREPARE_NEGATIVE_ID;
    }

    // Create the Row struct row_to_insert property within the Statement struct and add the id property
    statement->row_to_insert.id = id;

    // Note on copying strings compared to integers in C:
    // What would be talked about as "string" in C is just an array of characters stored contiguously in memory,
    // with a pointer to the first character and a NULL at the end to determine the end of the "string".
    // (note that this is not exactly infer that string are a linked lists, since linked lists do not imply a
    // contiguous memory allocation for every next character)

    // When you assign a string to another string, you can't simply copy the base address of the array
    // because this would only copy the reference to the first character of the string (i.e., the pointer),
    // not the actual characters of the string.
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

/*
 * Executes certain actions based on the input buffer passed in. Returns a value from
 * the prepare enum
 */
PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement)
{
    // Tries to match 6 characters of the buffer, since insert will be followed
    // by more data afterwards the keyword
    if (strncmp(input_buffer->buffer, "insert", 6) == 0)
    {
        return prepare_insert(input_buffer, statement);
    }
    if (strcmp(input_buffer->buffer, "select") == 0)
    {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}
/*
 *
 */
ExecuteResult execute_insert(Statement *statement, Table *table)
{
    void *node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = (*leaf_node_num_cells(node));
    if (num_cells >= LEAF_NODE_MAX_CELLS)
    {
        return EXECUTE_TABLE_FULL;
    }

    Row *row_to_insert = &(statement->row_to_insert);
    uint32_t key_to_insert = row_to_insert->id;
    Cursor *cursor = table_find(table, key_to_insert);

    if (cursor->cell_num < num_cells)
    {
        uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
        if (key_at_index == key_to_insert)
        {
            return EXECUTE_DUPLICATE_KEY;
        }
    }
    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);
    return EXECUTE_SUCCESS;
}

/*
 *
 */
ExecuteResult execute_select(Statement *statement, Table *table)
{
    Row row;
    Cursor *cursor = get_start_of_table_cursor(table);

    // Up until the end_of_table variable is set to true
    while (!(cursor->end_of_table))
    {
        void *cursor_value = get_cursor_value(cursor);

        // Deserialize the row (convert a linear bite array into structured data). Copy the row data on the required memory offset
        deserialize_row(cursor_value, &row);
        print_row(&row);
        cursor_advance(cursor);
    }
    return EXECUTE_SUCCESS;
}

/*
 * Switch statement that executes functions based on the statement type. Each function makes
 * this function return a value from the ExecuteResult
 */
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

/*
 * Reads standard CLI I/O to assign rthe necessary length of the buffer to the input_buffer object
 */
void read_input(InputBuffer *input_buffer)
{
    ssize_t bytes_read =
        getline(                            // Stdio C function to read data
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

    // Assign number of bytes read to input_length
    int buffer_to_assign = bytes_read - 1; // Ignore trailing newline (\n)(substract 1)
    input_buffer->input_length = buffer_to_assign;
    // Go up until wherever the buffer would reach and assign it to 0.
    input_buffer->buffer[buffer_to_assign] = 0;
}

void close_input_buffer(InputBuffer *input_buffer)
{
    free(input_buffer->buffer);
    free(input_buffer);
}

void print_prompt() { printf("db > "); }

void print_row(Row *row)
{
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void print_constants()
{
    printf("ROW SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void indent(uint32_t level)
{
    for (uint32_t i = 0; i < level; i++)
    {
        printf("  ");
    }
}

void print_tree(Pager *pager, uint32_t page_num, uint32_t indentation_level)
{
    void *node = get_page(pager, page_num);
    uint32_t num_keys, child;

    switch (get_node_type(node))
    {
    case (NODE_LEAF):
        num_keys = *leaf_node_num_cells(node);
        indent(indentation_level);
        printf("- leaf (size %d)\n", num_keys);
        for (uint32_t i = 0; i < num_keys; i++)
        {
            indent(indentation_level + 1);
            printf("- %d\n", *leaf_node_key(node, i));
        }
        break;
    case (NODE_INTERNAL):
        num_keys = *internal_node_num_keys(node);
        indent(indentation_level);
        printf("- internal (size %d)\n", num_keys);
        for (uint32_t i = 0; i < num_keys; i++)
        {
            child = *internal_node_child(node, i);
            print_tree(pager, child, indentation_level + 1);
            indent(indentation_level + 1);
            printf("- key %d\n", *internal_node_key(node, i));
        }
        child = *internal_node_right_child(node);
        print_tree(pager, child, indentation_level + 1);
        break;
    }
}

/*
 * Serializes a Row structure into a flat byte array.
 *
 * This function is used to convert the structured data within a Row structure
 * into a contiguous block of memory (byte array). This is useful for storing
 * the data in a format that can be written directly to disk or sent over a network.
 *
 * Parameters:
 *   source - Pointer to the Row structure to serialize.
 *   destination - Pointer to the buffer where the serialized data should be stored.
 */
void serialize_row(Row *row_source, void *destination)
{
    // Copy the ID field from the Row structure to the destination array at the specified ID_OFFSET.
    memcpy(destination + ID_OFFSET, &(row_source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(row_source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(row_source->email), EMAIL_SIZE);
}

/*
 * Deserializes a flat byte array into a Row structure.
 *
 * This function is used to convert a contiguous block of memory (byte array)
 * back into a structured Row format. This is useful for loading data from disk
 * or network into a structured format that the application can manipulate.
 *
 *  Parameters:
 *   source - Pointer to the buffer containing serialized data.
 *   destination - Pointer to the Row structure where the deserialized data should be stored.
 */
void deserialize_row(void *row_source, Row *destination)
{
    // Copy the ID field from the source array at the specified ID_OFFSET into the Row structure's ID field.
    memcpy(&(destination->id), row_source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), row_source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), row_source + EMAIL_OFFSET, EMAIL_SIZE);
}
/**
 * Initializes a leaf node by setting its cell count to zero.
 * This function is typically used when a new leaf node is created to ensure it
 * is in a clean state with no cells stored in it.
 *
 * @param node Pointer to the start of the leaf node to initialize.
 */
void initialize_leaf_node(void *node)
{
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
}

void initialize_internal_node(void *node)
{
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
}

void leaf_node_split_and_insert(Cursor *cursor, uint32_t key, Row *value)
{
    /*
        Create a new node and move half the cells over.
        Insert the new value in one of the two nodes.
        Update parent or create a new parent
    */

    void *old_node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
    void *new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);

    /*
  All existing keys plus new key should be divided
    evenly between old (left) and new (right) nodes.
    Starting from the right, move each key to correct position.
    */
    for (uint32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--)
    {
        void *destination_node;

        if (i >= LEAF_NODE_LEFT_SPLIT_COUNT)
        {
            destination_node = new_node;
        }
        else
        {
            destination_node = old_node;
        }
        uint32_t index_within_node = i % LEAF_NODE_SPLIT_COUNT;
        void *destination = leaf_node_cell(destination_node, index_within_node);
        if (i == cursor->cell_num)
        {
            serialize_row(value, destination);
        }
        else if (i > cursor->cell_num)
        {
            memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
        }
        else
        {
            memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    if (is_node_root(old_node))
    {
        return create_new_root(cursor->table, new_page_num);
    }
    else
    {
        printf("Need to implement updating parent after split\n");
        exit(EXIT_FAILURE);
    }
}

void leaf_node_insert(Cursor *cursor, uint32_t key, Row *value)
{
    void *node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS)
    {
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if (cursor->cell_num < num_cells)
    {
        // Make room for new cell
        for (uint32_t i = num_cells; i > cursor->cell_num; i--)
        {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_num)) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

int main(int argc, char *argv[])
{
    // If there are less than 2 arguments on the first prompt, it means no .db name has been specified.
    if (argc < 2)
    {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    // Second argument is the filename
    char *filename = argv[1];

    Table *table = db_open(filename);
    InputBuffer *input_buffer = new_input_buffer();

    while (true)
    {
        print_prompt();
        read_input(input_buffer);
        if (input_buffer->buffer[0] == '.')
        {
            switch (do_meta_command(input_buffer, table))
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
        case (PREPARE_STRING_TOO_LONG):
            printf("String too long.\n");
            continue;
        case (PREPARE_NEGATIVE_ID):
            printf("Row ID is negative, which is not allowed.\n");
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
        case (EXECUTE_DUPLICATE_KEY):
            printf("Error duplicated key. \n");
            break;
        case (EXECUTE_TABLE_FULL):
            printf("Error table full. \n");
            break;
        }
    }
}

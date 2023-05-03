/*main.c*/

//
// Project 02: Schema and AST output for SimpleSQL
// Benjamin Smith
// Northwestern University
// CS 211, Winter 2023
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>  // true, false
#include <ctype.h>
#include <string.h>
#include <strings.h>
#include "parser.h"
#include "scanner.h"
#include "analyzer.h"
#include "database.h"
#include "token.h"    // token defs
#include "scanner.h"  // scanner
#include "util.h"     // panic
#include "ast.h"

// #include files



// your functions

void print_schema(struct Database *db){ //prints the array of structures
  printf("Database: %s\n", db->name);
  int i, j;
  char indexed[16];
  char col[10];
  for(i = 0; i < db->numTables; i++) {
    printf("Table: %s\n  Record size: %d\n", db->tables[i].name, db->tables[i].recordSize);
    for(j = 0; j < db->tables[i].numColumns; j++) {
      printf("  Column: %s, ", db->tables[i].columns[j].name);
      if (db->tables[i].columns[j].colType == 1){
        printf("int, ");
      }
      else if(db->tables[i].columns[j].colType ==2){
        printf("real, ");
      }
      else {
        printf("string, ");
      }
      if (db->tables[i].columns[j].indexType == 0){
        printf("non-indexed\n");
      }
      else if (db->tables[i].columns[j].indexType == 1){
        printf("indexed\n");
      }
      else{
        printf("unique indexed\n");
      }
    }
  }
}
void print_ast(struct QUERY* query, struct Database *db){ //Prints the information contained in the AST
  struct SELECT* select = query->q.select;
  struct JOIN* join = select->join;
  struct WHERE* where = select->where;
  struct ORDERBY* orderby = select->orderby;
  struct LIMIT* limit = select->limit;
  struct INTO* into = select->into;
  struct COLUMN* column = select->columns;
  printf("**QUERY AST**\n");
  printf("Table: %s", select->table);
  while(column!=NULL){
  
  if(column->function == -1){
    printf("\nSelect column: %s.%s", column->table, column->name);
    column = column->next;
  }
  else if(column->function == 0){
    printf("\nSelect column: MIN(%s.%s)", column->table, column->name);
    column = column->next;
  }
  else if(column->function == 1){
    printf("\nSelect column: MAX(%s.%s)", column->table, column->name);
    column = column->next;
  }
  else if(column->function == 2){
    printf("\nSelect column: SUM(%s.%s)", column->table, column->name);
    column = column->next;
  }
  else if(column->function == 3){
    printf("\nSelect column: AVG(%s.%s)", column->table, column->name);
    column = column->next;
  }
  else if(column->function == 4){
    printf("\nSelect column: COUNT(%s.%s)", column->table, column->name);
    column = column->next;
  }
    }
  
  
  if(select->join !=NULL){
    printf("\nJoin %s On %s.%s = %s.%s\n", join->table, join->left->table, join->left->name,
    join->right->table, join->right->name);   
  }
  else{
    printf("\nJoin (NULL)\n");
  }
  if(select->where !=NULL){
    char op[50];
    struct EXPR* express = where->expr;
    if(express->operator == EXPR_LT){
      strcpy(op, "<");
    }
    else if(express->operator == EXPR_LTE){
      strcpy(op,"<=");
    }
    else if(express->operator == EXPR_GT){
      strcpy(op, ">");
    }
    else if(express->operator == EXPR_GTE){
      strcpy(op, ">=");
    }
    else if(express->operator == EXPR_EQUAL){
      strcpy(op, "=");
    }
    else if(express->operator == EXPR_LIKE){
      strcpy(op, "like");
    }
    else if(express->operator ==EXPR_NOT_EQUAL){
      strcpy(op, "<>");
    }
    if(express->litType == STRING_LITERAL){
      if(strchr(express->value, '\'')== NULL){
        printf("Where %s.%s %s '%s'\n", express->column->table, express->column->name, op, express->value);
      }
      else{
        printf("Where %s.%s %s \"%s\"\n", express->column->table, express->column->name, op, express->value);
      }
    }
    else {
      printf("Where %s.%s %s %s\n", express->column->table, express->column->name, op, express->value);
    }
  }
  else{
    printf("Where (NULL)\n");
  }
  if(select->orderby !=NULL){
    if(orderby->ascending){
      if(orderby->column->function == -1){
      printf("Order By %s.%s %s\n", orderby->column->table, orderby->column->name, "ASC");
        }
      else if(orderby->column-> function == 0){
        printf("Order By MIN(%s.%s) %s\n", orderby->column->table, orderby->column->name, "ASC");
      }
      else if(orderby->column-> function == 1){
        printf("Order By MAX(%s.%s) %s\n", orderby->column->table, orderby->column->name, "ASC");
      }
      else if(orderby->column-> function == 2){
        printf("Order By SUM(%s.%s) %s\n", orderby->column->table, orderby->column->name, "ASC");
      }
      else if(orderby->column-> function == 3){
        printf("Order By AVG(%s.%s) %s\n", orderby->column->table, orderby->column->name, "ASC");
      }
      else if(orderby->column-> function == 4){
        printf("Order By COUNT(%s.%s) %s\n", orderby->column->table, orderby->column->name, "ASC");
      }
    }
    if(!orderby->ascending){
      if(orderby->column-> function == -1){
        printf("Order By %s.%s %s\n", orderby->column->table, orderby->column->name, "DESC");
      }
      else if(orderby->column-> function == 0){
        printf("Order By MIN(%s.%s) %s\n", orderby->column->table, orderby->column->name, "DESC");
      }
      else if(orderby->column-> function == 1){
        printf("Order By MAX(%s.%s) %s\n", orderby->column->table, orderby->column->name, "DESC");
      }
      else if(orderby->column-> function == 2){
        printf("Order By SUM(%s.%s) %s\n", orderby->column->table, orderby->column->name, "DESC");
      }
      else if(orderby->column-> function == 3){
        printf("Order By AVG(%s.%s) %s\n", orderby->column->table, orderby->column->name, "DESC");
      }
       else if(orderby->column-> function == 4){
        printf("Order By COUNT(%s.%s) %s\n", orderby->column->table, orderby->column->name, "DESC");
      }
    }
  }
  else{
    printf("Order By (NULL)\n");
  }
  if(select->limit !=NULL){
    printf("Limit %d\n",limit->N);
  }
  else{
    printf("Limit (NULL)\n");
  }
  if(select->into !=NULL){
    printf("Into %s\n", into->table);
  }
  else{
    printf("Into (NULL)\n");
  }
  printf("**END OF QUERY AST**\n");
}
void execute_query(struct Database* database, struct QUERY* query) {
  
  char* database_name = database->name;
  char* query_table_name = query->q.select->table;
  int table_record_size = 0;
  char* directory_table_name;
  char* table_name;
  
  for (int i = 0; i < database->numTables; i++) {
    table_name = database->tables[i].name;
 
    if (strcasecmp(table_name, query_table_name) == 0) {
      directory_table_name = table_name;
      table_record_size = database->tables[i].recordSize;
    }
  }
  
  char path[(2 * DATABASE_MAX_ID_LENGTH) + 10];
  strcpy(path, database_name);
  strcat(path, "/");
  strcat(path, directory_table_name);
  strcat(path, ".data");

  FILE* fptr = fopen(path, "r");
  if (!fptr) {
    printf("Error!");
  }
  
  char* buffer = (char*) malloc(sizeof(char) * table_record_size + 3);
  if (!buffer) {
    panic("Out of memory\n");
  }
   
  for (int i = 0; i< 5; i++) { 
    fgets(buffer, table_record_size + 3, fptr);
    printf("%s", buffer);
  }
   
  free(buffer);
  fclose(fptr);
}  
int main(int argc, char *argv[]) {
  char filename[DATABASE_MAX_ID_LENGTH +1];
  struct Database *db;
  
  printf("database? ");
  scanf("%s", filename);
  db = database_open(filename); //attempts to open the file
  if(db==NULL){
    printf("**Error: unable to open database %s\n", filename); //if the file can't open
    exit(-1);
  }
  printf("**DATABASE SCHEMA**\n");
  print_schema(db); //Calls the print_schema function on the file
  printf("**END OF DATABASE SCHEMA**\n");
  parser_init(); // initialize the parser
  while(true){
    printf("query? ");
    struct TokenQueue* tokens = parser_parse(stdin); //parser uses the scanner to input the query
    if(tokens==NULL){ //semantic error
      if(parser_eof()){
        break;
      }
      else{
        continue;
      }
      }
    struct QUERY* ast = analyzer_build(db, tokens); //check for semantic errors
    if(ast == NULL){
      continue;
    }
    print_ast(ast, db); //prints the information in the AST
    execute_query(db,ast); //prints the first 5 lines of the data file
   }
    database_close(db);
    return 0;
}

  
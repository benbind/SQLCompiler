/*execute.c*/

//
// Project: Execution of queries for SimpleSQL
//
// Benji Smith
// Northwestern University
// CS 211, Winter 2023
//

#include <stdbool.h> // true, false
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
//
// #include any other system <.h> files?
//

#include "database.h"
#include "ast.h"
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
#include "execute.h"
#include "resultset.h"
//
// #include any other of our ".h" files?
//
static int getIndex(struct TableMeta* table, char* name){ //helper to get index
  for(int i = 0; i < table->numColumns; i++){
    if (icmpStrings(name, table -> columns[i].name) == 0){
      return i;
    }
    
}
  panic("No index found");
  return -1;
  }

int deleteUnwanted(struct TableMeta* tablemeta, struct ResultSet* rs, struct COLUMN* current){
  for(int i = 0; i<tablemeta->numColumns; i++){ //get rid of unwanted columns
    
    bool wanted = false;
    while(current){
      if(icmpStrings(tablemeta->columns[i].name, current->name) ==0){
        wanted = true;
        break;
      }
      current = current->next;
    
  }
  if(!wanted){
    resultset_deleteColumn(rs, resultset_findColumn(rs, 1, tablemeta->name, tablemeta->columns[i].name ));
  }
    }
}
void execute_query(struct Database* db, struct QUERY* query)
{
  char filename[2*DATABASE_MAX_ID_LENGTH +8];
  filename[0] =0;
  char* buffer = 0;
  struct ResultSet *rs;
  int bufferSize = 0;
  int query_table_inc = 0;
  char* cur = 0;
  if (db == NULL) panic("db is NULL (execute)");
  if (query == NULL) panic("query is NULL (execute)");
  if(!db){
    panic("Error db null\n");
  }
  if(!query){
    panic("Error query null\n");
  }
  if (query->queryType != SELECT_QUERY)
  {
    printf("**INTERNAL ERROR: execute() only supports SELECT queries.\n");
    return;
    }
  struct SELECT* select = query->q.select;  // alias for less typing:
   //
  // the query has been analyzed and so we know it's correct: the
  // database exists, the table(s) exist, the column(s) exist, etc.
  //

  //
  // (1) we need a pointer to the table meta data, so find it:
  //
  struct TableMeta* tablemeta = NULL;

  for (int t = 0; t < db->numTables; t++)
  {
    if (icmpStrings(db->tables[t].name, select->table) == 0)  // found it:
    {
      tablemeta = &db->tables[t];
      break;
    }
    
  }
  
  assert(tablemeta != NULL);
  // results = resultset_create();
  // for (int i =0; i<tablemeta->numColumns; i++){
  //   resultset_insertColumn(results, i+i, tablemeta->name, tablemeta->columns[i].name, NO_FUNCTION, tablemeta->columns[i].colType);
  // }
  
  // 
  // (2) open the table's data file
  //
  // the table exists within a sub-directory under the executable
  // where the directory has the same name as the database, and with 
  // a "TABLE-NAME.data" filename within that sub-directory:
  //
  char path[(2 * DATABASE_MAX_ID_LENGTH) + 10];

  strcpy(path, db->name);    // name/name.data
  strcat(path, "/");
  strcat(path, tablemeta->name);
  strcat(path, ".data");

  FILE* datafile = fopen(path, "r");
  if (datafile == NULL) // unable to open:
  {
    printf("**INTERNAL ERROR: table's data file '%s' not found.\n", path);
    panic("execution halted");
  }

  //
  // (3) allocate a buffer for input, and start reading the data:
  //
  int   dataBufferSize = tablemeta->recordSize + 3;  // ends with $\n + null terminator
  char* dataBuffer = (char*)malloc(sizeof(char) * dataBufferSize);
  if (dataBuffer == NULL) panic("out of memory");

  rs = resultset_create(); // make resultset
  strcat(filename, db->name);
  strcat(filename, "/"); // make file name

  for (int i = 0; i<db->numTables; i++){
    if (strcasecmp(db->tables[i].name, query->q.select->table) == 0) {
      strcat(filename, db->tables[i].name);
      bufferSize = db->tables[i].recordSize + 3; // 3 for $\n and null terminator
      break;
    }
  }
  //begin step 4
  strcat(filename, ".data");
  assert(bufferSize);
  buffer = (char*) malloc(sizeof(char) *bufferSize);
  if(buffer == NULL){
    panic("Out of memory\n");
  }
  FILE* data_file = fopen(filename, "r");
  if ( data_file == NULL){
    printf("**ERROR: unable to open '%s'.\n", filename);
  }
  for (int i = 0; i<db->numTables; i++){
    if (strcasecmp(db->tables[i].name, query->q.select->table)==0){
      query_table_inc = i;
    }
   }
  for(int i = 1; i< tablemeta->numColumns+1; i++){
    resultset_insertColumn(rs, i, tablemeta->name, tablemeta->columns[i-1].name, NO_FUNCTION, tablemeta->columns[i-1].colType);
  }
  int counter = 0;
  while(true){
    counter = counter+1;
    fgets(buffer, bufferSize, data_file); //read whole file
    if(feof(data_file)){
      break;
    }
    else{ //search through to find a new line or and end of quote
      cur = buffer;
      while(*cur!='\0'){
        // printf("%s", cur);
        if(*cur == ' '){ //make all the spaces null terminators
          *cur = '\0';
          cur = cur+1;
          
        }
        else if (*cur == '\''){
          *cur = '\0';
          cur = cur+1;
          while( *cur!='\''){
            cur = cur+1;
          }
          *cur = '\0';
          cur = cur+1;
          *cur = '\0';
          cur = cur+1;
        }
        else if (*cur == '"'){
          *cur = '\0';
          cur = cur+1;
          while (*cur != '"'){
            cur = cur+1;
          }
          *cur = '\0';
          cur = cur+1;
          *cur = '\0';
          cur = cur+1;
          
        }
        else{
          cur = cur+1;
          }
      }
    }
    
  cur = buffer;
  resultset_addRow(rs); //add everything
  for(int i = 0; i<tablemeta->numColumns; i++){
    // printf("%s\n", cur);
    if(tablemeta->columns[i].colType == COL_TYPE_INT){
      resultset_putInt(rs, counter, i+1, atoi(cur));
      
    }
    if(tablemeta->columns[i].colType == COL_TYPE_REAL){
      resultset_putReal(rs, counter, i+1, atof(cur));
    }
    if(tablemeta->columns[i].colType == COL_TYPE_STRING){
      resultset_putString(rs, counter, i+1, cur);
    }
    while(*cur != '\0'){
      cur = cur+1;
    }
    cur = cur+1;
    if(*cur == '\0'){
      cur = cur+1;
    }
    if(*cur == '\0'){
      cur = cur+1;
    }
    
  }
    }
    
  
  
  free(buffer); 
  fclose(data_file);
  

struct TableMeta* tables = db->tables;
if(select->where != NULL){ //start going into where
  int index = getIndex(tablemeta, select->where->expr->column->name);
  assert(index >= 0);
  assert(index < tablemeta->numColumns);
  double diff =0;
  for(int i = rs->numRows; i > 0; i--){
  //int col = resultset_findColumn(rs, 1, tablemeta->name,
//select->where->expr->column->name);
  int type = (tablemeta->columns[index].colType);
  if(type == COL_TYPE_INT){ //check int vs. real vs. string
    int value = resultset_getInt(rs, i, index+1);
    int val2 = atoi(select->where->expr->value);
    diff = value - val2;
  }
  else if (type == COL_TYPE_REAL){
    double value = resultset_getReal(rs, i, index+1);
    double val2 = atof(select->where->expr->value);
    diff = value - val2;
  }
  else if (type == COL_TYPE_STRING){
    char* value = resultset_getString(rs, i, index+1);
    char* val2 = (select->where->expr->value);
    diff = strcmp(value, val2);
    free(value);
  }
  if (select->where->expr->operator == EXPR_LT){
    if(diff >= 0){
      resultset_deleteRow(rs, i); //delete if it doesnt fit the operator, same for items below
    }
  } 
  else if (select->where->expr->operator == EXPR_LTE){
    if(diff > 0){
      resultset_deleteRow(rs, i);
    }
  }
  else if (select->where->expr->operator == EXPR_GT){
    if(diff <= 0){
      resultset_deleteRow(rs, i);
    }
  }
  else if (select->where->expr->operator == EXPR_GTE){
    if(diff < 0){
      resultset_deleteRow(rs, i);
    }
  }
  else if (select->where->expr->operator == EXPR_EQUAL){
    if(diff != 0){
      resultset_deleteRow(rs, i);
    }
  }
  else if (select->where->expr->operator == EXPR_NOT_EQUAL){
    if(diff == 0){
      resultset_deleteRow(rs, i);
    }
  }
    }
  }
  struct COLUMN* current = select->columns;
  deleteUnwanted(tablemeta, rs, current);
  // for(int i = 0; i<tablemeta->numColumns; i++){ //get rid of unwanted columns
  //   struct COLUMN* current = select->columns;
  //   bool wanted = false;
  //   while(current){
  //     if(icmpStrings(tablemeta->columns[i].name, current->name) ==0){
  //       wanted = true;
  //       break;
  //     }
  //     current = current->next;
    
  // }
  // if(!wanted){
  //   resultset_deleteColumn(rs, resultset_findColumn(rs, 1, tablemeta->name, tablemeta->columns[i].name ));
  // }
  //   }
  
  //reorder the columns based on input
  // struct COLUMN* current = select->columns;
  int x = 1;
  while(current){
    int column = resultset_findColumn(rs, 1, current->table, current->name);
    resultset_moveColumn(rs, column, x);
    x = x+1;
    current = current->next;
  }
  int i = 1;
  struct COLUMN* current1 = select->columns; //apply functions based on input
  while(current1 != NULL){
    if(current1->function != NO_FUNCTION){
      resultset_applyFunction(rs, current1->function, i);
      }
      current1 = current1->next;
      i++;
  }
  //get rid of everything higher than the input
  struct LIMIT* limit = select->limit;
  struct COLUMN* cur2 = select->columns;
  int j = 1;
  // while(cur2 != NULL){
    
  //   if(j >= limit->N){
  //     resultset_deleteRow(rs, j);
  //   }
  //   cur2 = cur2->next;
  // }
  if(limit != NULL){
    for(int i = rs->numRows; i > limit->N; i--){
      resultset_deleteRow(rs, i);
    }
  }
  // while (true)
  // {
  //   fgets(dataBuffer, dataBufferSize, datafile);

  //   if (feof(datafile)) // end of the data file, we're done
  //     break;

  //   printf("%s", dataBuffer);
  // }

  //
  // done!
  //
  

  resultset_print(rs);
  resultset_destroy(rs);
  free(dataBuffer);
  fclose(datafile);
  }
  
//
// implementation of function(s), both private and public
//

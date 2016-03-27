header {package decaf;}

options
{
  mangleLiteralPrefix = "TK_";
  language="Java";
}

class DecafParser extends Parser;
options
{
  importVocab=DecafScanner;
  k=3;
  buildAST=true;
}

program: TK_class ID LCURLY (field_decl)* (method_decl)* RCURLY EOF;

field_decl: type (ID | ID LBRACKET INT_LITERAL RBRACKET)
            (COMMA ID | ID LBRACKET INT_LITERAL RBRACKET)* SEMICOLON;

method_decl: (type | TK_void) ID LPARENT ((type ID)(COMMA type ID)*)? RPARENT block;

block: LCURLY (var_decl)* (statement)* RCURLY;

var_decl: type ID (COMMA ID)* SEMICOLON;

type: TK_int | TK_boolean;

assign_op
        : EQUAL_ASSIGN
        | PLUS_ASSIGN
        | MINUS_ASSIGN;

statement
        : location assign_op expr SEMICOLON
        | method_call SEMICOLON
        | TK_if LPARENT expr RPARENT block (TK_else block)?
        | TK_for ID EQUAL_ASSIGN expr COMMA expr block
        | TK_return (expr)? SEMICOLON
        | TK_break SEMICOLON
        | TK_continue SEMICOLON
        | block;

method_call
          : method_name LPARENT (expr (COMMA expr)*)? RPARENT
          | TK_callout LPARENT STRING_LITERAL (COMMA callout_arg)* RPARENT;

method_name: ID;

location
        : ID
        | ID LBRACKET expr RBRACKET;

expr
    : (location
    | method_call
    | literal
    | MINUS expr
    | NOT expr
    | LPARENT expr RPARENT) (bin_op expr)?;

callout_arg
          : expr
          | STRING_LITERAL;

bin_op
      : arith_op
      | rel_op
      | eq_op
      | cond_op;

arith_op
        : PLUS
        | MINUS
        | MULTIPLY
        | DIVIDE
        | MODULUS;

rel_op
      : LT
      | GT
      | LTEQ
      | GTEQ;

eq_op
      : EQ
      | NEQ;

cond_op
      : AND
      | OR;

literal
      : INT_LITERAL
      | CHAR_LITERAL
      | BOOL_LITERAL;

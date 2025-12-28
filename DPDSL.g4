grammar DPDSL;

/* ===== PARSER RULES ===== */

query
    : select_clause from_clause? group_by_clause? where_clause? EOF
    ;

select_clause
    : SELECT select_item (',' select_item)*
    ;

select_item
    : expression (AS? identifier)?
    ;

expression
    : function_name '(' '*' ')'                               # CountStar
    | function_name '(' expression (OF '[' budget ']')? ')'   # Aggregation
    | label identifier                                        # ColumnRef
    | expression operator expression                          # BinaryOp
    | '(' expression ')'                                      # Parens
    | INT                                                     # Literal
    | FLOAT                                                   # FloatLiteral
    ;

from_clause
    : FROM identifier
    ;

where_clause
    : WHERE expression
    ;

group_by_clause
    : GROUP BY columnRef (',' columnRef)*
    ;

columnRef
    : label identifier
    ;

/* ===== LEXER RULES ===== */

// Keywords (must come before identifier to have precedence)
SELECT: 'SELECT' ;
FROM: 'FROM' ;
WHERE: 'WHERE' ;
GROUP: 'GROUP' ;
BY: 'BY' ;
AS: 'AS' ;
OF: 'OF' ;

// Labels
label: PRIVATE | PUBLIC ;
PRIVATE: 'PRIVATE' ;
PUBLIC: 'PUBLIC' ;

// Functions
function_name: SUM | COUNT | MAX | AVG | MIN ;
SUM: 'SUM' ;
COUNT: 'COUNT' ;
MAX: 'MAX' ;
AVG: 'AVG' ;
MIN: 'MIN' ;

// Operators
operator: '+' | '-' | '*' | '/' | '>' | '<' | '=' | '>=' | '<=' | '!=' ;

// Budget (epsilon value)
budget: FLOAT | INT ;

// Identifiers and Literals
identifier: ID ;
ID: [a-zA-Z_][a-zA-Z0-9_]* ;

INT: [0-9]+ ;
FLOAT: [0-9]+ '.' [0-9]+ ;

// Whitespace
WS: [ \t\r\n]+ -> skip ;

// Comments (optional but helpful)
LINE_COMMENT: '--' ~[\r\n]* -> skip ;
BLOCK_COMMENT: '/*' .*? '*/' -> skip ;
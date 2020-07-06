
grammar Expression;

parse
 : expr EOF
 ;

exprs
 : expr (Comma expr)*
 ;

pair
 : expr Colon expr
 ;

as
 : name Assign expr
 ;

// Important! must list all name-like tokens here

name
 : (Identifier | With | For | In | Where | Any | All | Of | Like | ILike)
 ;

expr
 : expr (Dot Identifier)? LParen exprs ? RParen #exprCall
// | expr Dot Mul Dot Identifier #exprStarMember
 | expr Dot Identifier #exprMember
 | expr LSquare expr RSquare #exprIndex
 | op=(Sub | Not | BitNot) expr #exprUnary
 | <assoc=right> expr Pow expr #exprPow
 | expr op=(Mul | Div | Mod) expr #exprMul
 | expr op=(Add | Sub ) expr #exprAdd
 | expr op=(BitLsh | BitRsh) expr #exprBitSh
 | expr Cmp expr #exprCmp
 | expr op=(Gte | Lte | Gt | Lt) expr #exprRel
 | expr op=(Eq | Ne) expr #exprEq
 | expr BitAnd expr  #exprBitAnd
 | expr BitXor expr #exprBitXor
 | expr BitOr expr #exprBitOr
 | expr In expr #exprIn
 | expr Identifier expr #exprOperator
 | expr op=(Like | ILike) expr #exprLike
 | expr And expr #exprAnd
 | expr Or expr #exprOr
 | <assoc=right> expr QQMark expr #exprCoalesce
 | expr QMark expr Colon expr #exprIfElse
 | LBrace expr Colon expr For expr RBrace #exprForObject
 | LBrace expr For expr RBrace #exprForSet
 | LSquare expr For expr RSquare #exprForArray
 | expr For Any expr #exprForAny
 | expr For All expr #exprForAll
 | With LParen as (Comma as)* RParen expr #exprWith
 | Number #exprNumber
 | Bool #exprBool
 | Null #exprNull
 | LSquare exprs? RSquare #exprArray
 | LBrace exprs? RBrace #exprSet
 | LBrace (pair (Comma pair)*)? RBrace #exprObject
 | Identifier (Dot Identifier)*? #exprPathConstant
 | String #exprString
 | LParen expr RParen #exprExpr
 | (name | (LParen name (Comma name)* RParen)) Of expr #exprOf
 | expr Where expr #exprWhere
 | (name | (LParen name (Comma name)* RParen)) Arrow expr #exprLambda
 ;

Null     : N U L L;
In       : I N;
For      : F O R;
Of       : O F;
Where    : W H E R E;
With     : W I T H;
Any      : A N Y;
All      : A L L;
Like     : L I K E;
ILike    : I L I K E;

Arrow    : '->';
Or       : '||';
And      : '&&';
BitOr    : '|';
BitAnd   : '&';
BitXor   : '^';
BitNot   : '~';
BitLsh   : '<<';
BitRsh   : '>>';
Cmp      : '<=>';
Eq       : '==';
Ne       : '!=';
Gte      : '>=';
Lte      : '<=';
Pow      : '**';
QQMark   : '??';
Not      : '!';
Gt       : '>';
Lt       : '<';
Add      : '+';
Sub      : '-';
Mul      : '*';
Div      : '/';
Mod      : '%';
LBrace   : '{';
RBrace   : '}';
LSquare  : '[';
RSquare  : ']';
LParen   : '(';
RParen   : ')';
Comma    : ',';
QMark    : '?';
Colon    : ':';
Dot      : '.';
Assign   : '=';

Bool
 : T R U E
 | F A L S E
 ;

Number
 : Int ( '.' Digit* )?
 ;

Identifier
 : [a-zA-Z_$] [a-zA-Z_0-9$]*
 ;

String
 : ["] ( ~["\r\n\\] | '\\' ~[\r\n] )* ["]
 | ['] ( ~['\r\n\\] | '\\' ~[\r\n] )* [']
 ;

Comment
 : ( '/*' .*? '*/' ) -> skip
 ;

Space
 : Ws -> skip
 ;

fragment Ws
 : [ \t\r\n\u000C]
 ;

fragment Int
 : [1-9] Digit*
 | '0'
 ;

fragment Digit
 : [0-9]
 ;

fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');

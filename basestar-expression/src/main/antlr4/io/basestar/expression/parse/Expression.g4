
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
 : identifier Assign expr
 ;

// Important! must list all name-like tokens here

identifier
 : (Identifier | With | For | In | Where | Any | All | Of | Like | ILike | Select | From | Union | Join | Left | Right | Inner | Outer | As | Group | Order | By)
 ;

name
 : Identifier (Dot Identifier)*?
 ;

names
 : name (Comma name)*
 ;

where
 : Where expr
 ;

of
 : (identifier | (LParen identifier (Comma identifier)* RParen)) Of expr where?
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
 | expr op=(Like | ILike) expr #exprLike
 | expr And expr #exprAnd
 | expr Or expr #exprOr
 | <assoc=right> expr QQMark expr #exprCoalesce
 | expr QMark expr Colon expr #exprIfElse
 | LBrace expr Colon expr For of RBrace #exprForObject
 | LBrace expr For of RBrace #exprForSet
 | LSquare expr For of RSquare #exprForArray
 | expr For Any of #exprForAny
 | expr For All of #exprForAll
 | With LParen as (Comma as)* RParen expr #exprWith
 | Number #exprNumber
 | Bool #exprBool
 | Null #exprNull
 | LSquare exprs? RSquare #exprArray
 | LBrace exprs? RBrace #exprSet
 | LBrace (pair (Comma pair)*)? RBrace #exprObject
 | name #exprNameConstant
 | String #exprString
 | LParen expr RParen #exprExpr
 | (identifier | (LParen identifier (Comma identifier)* RParen)) Arrow expr #exprLambda
 | Select exprs From expr (Where expr)? (Group By names)? (Order By names)? #exprSelect
 | expr side=(Left | Right)* type=(Inner | Outer)* Join expr #exprJoin
 | expr Union expr #exprUnion
 | expr As Identifier #exprAs
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
Select   : S E L E C T;
From     : F R O M;
Union    : U N I O N;
Join     : J O I N;
Left     : L E F T;
Right    : R I G H T;
Inner    : I N N E R;
Outer    : R I G H T;
As       : A S;
Group    : G R O U P;
Order    : O R D E R;
By       : B Y;

Arrow    : '=>';
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

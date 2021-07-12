
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

// Important! must list all name-like tokens here

identifier
 : (Identifier | With | For | In | Where | Any | All | Of | Like | ILike | Select | From | Union | Join | Left | Right | Inner | Outer | As | Group | Order | By |  Cast | Full)
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

typeExpr
 : identifier
 ;

withExprs
 : withExpr (Comma withExpr)*
 ;

withExpr
 : identifier As expr
 ;

caseExpr
 : When expr Then expr
 ;

//selectExpr
// : namedExpr
// ;
//
//fromExpr
// : expr
// ;

selectExpr
 : Mul #selectAll
 | expr #selectAnon
 | expr As? identifier #selectNamed
 ;

selectExprs
 : selectExpr (Comma selectExpr)*
 ;

fromExpr
 : expr #fromAnon
 | expr As? identifier #fromNamed
 | fromExpr Inner? Join fromExpr On expr #fromInnerJoin
 | fromExpr Left Outer? Join fromExpr On expr #fromLeftOuterJoin
 | fromExpr Right Outer? Join fromExpr On expr #fromRightOuterJoin
 | fromExpr Full Outer? Join fromExpr On expr #fromFullOuterJoin
 ;

fromExprs
 : fromExpr (Comma fromExpr)*
 ;

unionExpr
 : Union expr #unionDistinct
 | Union All expr #unionAll
 ;

sorts
 : sort (Comma sort)*
 ;

sort
 : name order=(Asc | Desc)?
 ;

expr
 : expr (Dot Identifier)? LParen exprs ? RParen #exprCall
 | Cast LParen expr As typeExpr RParen #exprCast
// | expr Dot Mul Dot Identifier #exprStarMember
 | expr Dot Identifier #exprMember
 | expr LSquare expr RSquare #exprIndex
 | op=(Sub | Bang | Not | Tilde) expr #exprUnary
 | <assoc=right> expr AstAst expr #exprPow
 | expr op=(Mul | Div | Mod) expr #exprMul
 | expr op=(Add | Sub ) expr #exprAdd
 | expr op=(BitLsh | BitRsh) expr #exprBitSh
 | expr Cmp expr #exprCmp
 | expr op=(Gte | Lte | Gt | Lt) expr #exprRel
 | expr op=(Eq | EqEq | BangEq) expr #exprEq
 | expr Amp expr  #exprBitAnd
 | expr (Xor | Caret) expr #exprBitXor
 | expr Pipe expr #exprBitOr
 | expr In expr #exprIn
 | expr op=(Like | ILike) expr #exprLike
 | expr (AmpAmp | And) expr #exprAnd
 | expr (PipePipe | Or) expr #exprOr
 | <assoc=right> expr QQMark expr #exprCoalesce
 | expr QMark expr Colon expr #exprIfElse
 | LBrace expr Colon expr For of RBrace #exprForObject
 | LBrace expr For of RBrace #exprForSet
 | LSquare expr For of RSquare #exprForArray
 | expr For Any of #exprForAny
 | expr For All of #exprForAll
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
 | Select selectExprs From fromExprs (Where expr)? (Group By exprs)? (Order By sorts)? unionExpr* #exprSelect
 | Case caseExpr+ (Else expr)? End #exprCase
 | With withExprs expr #exprWith
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
Outer    : O U T E R;
Full     : F U L L;
On       : O N;
As       : A S;
Group    : G R O U P;
Order    : O R D E R;
By       : B Y;
Cast     : C A S T;
And      : A N D;
Or       : O R;
Xor      : X O R;
Not      : N O T;
Case     : C A S E;
When     : W H E N;
Then     : T H E N;
Else     : E L S E;
End      : E N D;
Is       : I S;
Asc      : A S C;
Desc     : D E S C;

Arrow    : '=>';
PipePipe : '||';
AmpAmp   : '&&';
Pipe     : '|';
Amp      : '&';
Caret    : '^';
Tilde    : '~';
BitLsh   : '<<';
BitRsh   : '>>';
Cmp      : '<=>';
EqEq     : '==';
BangEq   : '!=';
Gte      : '>=';
Lte      : '<=';
AstAst   : '**';
QQMark   : '??';
Bang     : '!';
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
Eq       : '=';

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

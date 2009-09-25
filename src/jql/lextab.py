# lextab.py. This file automatically created by PLY (version 3.2). Don't edit!
_tabversion   = '3.2'
_lextokens    = {'QSTAR': 1, 'IN': 1, 'OMITNULL': 1, 'QNAME': 1, 'LBRACKET': 1, 'DEPTH': 1, 'COLON': 1, 'LIMIT': 1, 'NULL': 1, 'TRUE': 1, 'GROUPBY': 1, 'URI': 1, 'DIVIDE': 1, 'LE': 1, 'RPAREN': 1, 'EQ': 1, 'NE': 1, 'MINUS': 1, 'ASC': 1, 'LT': 1, 'PLUS': 1, 'COMMA': 1, 'NS': 1, 'ORDERBY': 1, 'GT': 1, 'STRING': 1, 'MAYBE': 1, 'IS': 1, 'PERIOD': 1, 'RBRACE': 1, 'TIMES': 1, 'GE': 1, 'LPAREN': 1, 'OFFSET': 1, 'VAR': 1, 'WHERE': 1, 'ID': 1, 'DESC': 1, 'AND': 1, 'LBRACE': 1, 'FALSE': 1, 'NAME': 1, 'INT': 1, 'FLOAT': 1, 'NOT': 1, 'RBRACKET': 1, 'OR': 1, 'MOD': 1}
_lexreflags   = 0
_lexliterals  = ''
_lexstateinfo = {'INITIAL': 'inclusive'}
_lexstatere   = {'INITIAL': [('(?P<t_INT>\\d+)|(?P<t_FLOAT>(\\d+)(\\.\\d+)?((e|E)(\\+|-)?(\\d+)))|(?P<t_STRING>(?:"(?:[^"\\n\\r\\\\]|(?:"")|(?:\\\\x[0-9a-fA-F]+)|(?:\\\\.))*")|(?:\'(?:[^\'\\n\\r\\\\]|(?:\'\')|(?:\\\\x[0-9a-fA-F]+)|(?:\\\\.))*\'))|(?P<t_URI><(([a-zA-Z][0-9a-zA-Z+\\-\\.]*:)/{0,2}[0-9a-zA-Z;/?:@&=+$\\.\\-_!~*\'()%]+)?(\\#[0-9a-zA-Z;/?:@&=+$\\.\\-_!~*\'()%]*)?>)|(?P<t_VAR>\\?[A-Za-z_\\$][\\w_\\$]*)|(?P<t_QNAME>(?P<prefix>[A-Za-z_\\$][\\w_\\$]*:)?(?P<name>[A-Za-z_\\$][\\w_\\$]*))|(?P<t_QSTAR>[A-Za-z_\\$][\\w_\\$]*:\\*)|(?P<t_comment>/\\*(.|\\n)*?\\*/)|(?P<t_linecomment>(//|\\#).*\\n)|(?P<t_NEWLINE>(\\n|\\r)+)|(?P<t_EQ>==?)|(?P<t_PLUS>\\+)|(?P<t_LBRACE>\\{)|(?P<t_RBRACKET>\\])|(?P<t_NE>!=)|(?P<t_LE><=)|(?P<t_LBRACKET>\\[)|(?P<t_LPAREN>\\()|(?P<t_TIMES>\\*)|(?P<t_GE>>=)|(?P<t_RPAREN>\\))|(?P<t_PERIOD>\\.)|(?P<t_RBRACE>\\})|(?P<t_LT><)|(?P<t_COMMA>,)|(?P<t_DIVIDE>/)|(?P<t_MOD>%)|(?P<t_MINUS>-)|(?P<t_GT>>)|(?P<t_COLON>:)', [None, ('t_INT', 'INT'), ('t_FLOAT', 'FLOAT'), None, None, None, None, None, None, ('t_STRING', 'STRING'), ('t_URI', 'URI'), None, None, None, ('t_VAR', 'VAR'), ('t_QNAME', 'QNAME'), None, None, ('t_QSTAR', 'QSTAR'), ('t_comment', 'comment'), None, ('t_linecomment', 'linecomment'), None, ('t_NEWLINE', 'NEWLINE'), None, (None, 'EQ'), (None, 'PLUS'), (None, 'LBRACE'), (None, 'RBRACKET'), (None, 'NE'), (None, 'LE'), (None, 'LBRACKET'), (None, 'LPAREN'), (None, 'TIMES'), (None, 'GE'), (None, 'RPAREN'), (None, 'PERIOD'), (None, 'RBRACE'), (None, 'LT'), (None, 'COMMA'), (None, 'DIVIDE'), (None, 'MOD'), (None, 'MINUS'), (None, 'GT'), (None, 'COLON')])]}
_lexstateignore = {'INITIAL': ' \t\x0c'}
_lexstateerrorf = {'INITIAL': 't_error'}

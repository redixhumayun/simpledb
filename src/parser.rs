use std::{error::Error, fmt::Display, iter::Peekable, str::Chars};

use crate::{ComparisonOp, Constant, Expression, Predicate, Term};

#[derive(Debug)]
enum ParserError {
    BadSyntax,
}

impl Error for ParserError {}

impl Display for ParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParserError::BadSyntax => write!(f, "Bad syntax"),
        }
    }
}

struct Parser<'a> {
    lexer: Lexer<'a>,
}

impl<'a> Parser<'a> {
    fn new(string: &'a str) -> Self {
        Self {
            lexer: Lexer::new(string),
        }
    }

    fn select_list(&mut self) -> Result<Vec<String>, ParserError> {
        let mut list = Vec::new();
        list.push(self.lexer.eat_identifier()?);
        while self.lexer.match_delim(',') {
            self.lexer.eat_delim(',')?;
            list.push(self.lexer.eat_identifier()?);
        }
        Ok(list)
    }

    fn select_tables(&mut self) -> Result<Vec<String>, ParserError> {
        let mut list = Vec::new();
        list.push(self.lexer.eat_identifier()?);
        while self.lexer.match_delim(',') {
            self.lexer.eat_delim(',')?;
            list.push(self.lexer.eat_identifier()?);
        }
        Ok(list)
    }

    fn constant(&mut self) -> Result<Constant, ParserError> {
        if self.lexer.match_string_constant() {
            return Ok(Constant::String(self.lexer.eat_string_constant()?));
        }
        return Ok(Constant::Int(self.lexer.eat_int_constant()?));
    }

    fn expression(&mut self) -> Result<Expression, ParserError> {
        if self.lexer.match_identifier() {
            return Ok(Expression::FieldName(self.lexer.eat_identifier()?));
        }
        return Ok(Expression::Constant(self.constant()?));
    }

    fn term(&mut self) -> Result<Term, ParserError> {
        let lhs = self.expression()?;
        let op = match self.lexer.current_token {
            Some(Token::Delimiter(Lexer::EQUAL)) => ComparisonOp::Equal,
            Some(Token::Delimiter(Lexer::GREATER)) => ComparisonOp::GreaterThan,
            Some(Token::Delimiter(Lexer::LESS)) => ComparisonOp::LessThan,
            _ => return Err(ParserError::BadSyntax),
        };
        self.lexer
            .next_token()
            .ok_or_else(|| ParserError::BadSyntax)?;
        let rhs = self.expression()?;
        Ok(Term::new_with_op(lhs, rhs, op))
    }

    fn terms(&mut self) -> Result<Vec<Term>, ParserError> {
        let mut terms = Vec::new();
        terms.push(self.term()?);
        //  TODO: Handle more boolean connectives
        while self.lexer.match_keyword("and") {
            self.lexer.eat_keyword("and")?;
            terms.push(self.term()?);
        }
        Ok(terms)
    }

    fn query(&mut self) -> Result<QueryData, ParserError> {
        self.lexer.eat_keyword("select")?;
        let select_list = self.select_list()?;
        self.lexer.eat_keyword("from")?;
        let table_list = self.select_tables()?;
        let predicate = {
            if self.lexer.match_keyword("where") {
                self.lexer.eat_keyword("where")?;
                let terms = self.terms()?;
                let predicate = Predicate::new(terms);
                predicate
            } else {
                Predicate::new(Vec::new())
            }
        };
        Ok(QueryData::new(select_list, table_list, predicate))
    }
}

#[cfg(test)]
mod parser_tests {
    use crate::{ComparisonOp, Constant, Expression, PredicateNode, Term};

    #[test]
    fn parse_basic_select_statement() {
        let sql = "SELECT name, age FROM users WHERE id = 3 AND name = 'John'";
        let mut parser = super::Parser::new(sql);
        let query_data = parser.query().unwrap();

        assert_eq!(query_data.fields, vec!["name", "age"]);
        assert_eq!(query_data.tables, vec!["users"]);
        matches!(
            query_data.predicate.root,
            PredicateNode::Composite { ref op, ref operands }
        );
        let PredicateNode::Composite { op, operands } = &query_data.predicate.root else {
            panic!("Expected Composite PredicateNode");
        };
        matches!(
            &operands[0],
            PredicateNode::Term(Term {
                lhs: Expression::FieldName(id),
                rhs: Expression::Constant(Constant::Int(val)),
                comparison_op: ComparisonOp::Equal,
            }) if *val == 3
        );
        matches!(
            &operands[1],
            PredicateNode::Term(Term {
                lhs: Expression::FieldName(name),
                rhs: Expression::Constant(Constant::String(val)),
                comparison_op: ComparisonOp::Equal
            }) if val == "john"
        );
    }
}

#[derive(Debug)]
struct QueryData {
    fields: Vec<String>,
    tables: Vec<String>,
    predicate: Predicate,
}

impl QueryData {
    fn new(fields: Vec<String>, tables: Vec<String>, predicate: Predicate) -> Self {
        Self {
            fields,
            tables,
            predicate,
        }
    }
}

struct Lexer<'a> {
    input: Peekable<Chars<'a>>,
    keywords: Vec<String>,
    current_token: Option<Token>,
}

impl<'a> Lexer<'a> {
    const EQUAL: char = '=';
    const GREATER: char = '>';
    const LESS: char = '<';

    fn new(string: &'a str) -> Self {
        let keywords = [
            "select", "from", "where", "and", "insert", "into", "values", "delete", "update",
            "set", "create", "table", "int", "varchar", "view", "as", "index", "on",
        ];
        let mut lexer = Self {
            input: string.chars().peekable(),
            keywords: keywords.iter().map(|s| s.to_lowercase()).collect(),
            current_token: None,
        };
        lexer.next_token().unwrap();
        lexer
    }

    fn parse_string(&mut self) -> Option<Token> {
        self.input.next(); //  consume the opening quote
        let mut string = String::new();
        while let Some(&c) = self.input.peek() {
            if c == '\'' {
                self.input.next(); //  consume the closing quote
                break;
            }
            string.push(c);
            self.input.next();
        }
        Some(Token::StringConstant(string))
    }

    fn parse_number(&mut self) -> Option<Token> {
        let mut number = String::new();
        while let Some(&c) = self.input.peek() {
            if !c.is_ascii_digit() {
                break;
            }
            number.push(c);
            self.input.next();
        }
        Some(Token::IntConstant(number.parse().unwrap()))
    }

    fn parse_identifier_or_keyword(&mut self) -> Option<Token> {
        let mut string = String::new();
        while let Some(&c) = self.input.peek() {
            if !c.is_alphabetic() && c != '_' {
                break;
            }
            string.push(c);
            self.input.next();
        }
        if self.keywords.contains(&string.to_lowercase()) {
            return Some(Token::Keyword(string.to_lowercase()));
        }
        Some(Token::Identifier(string.to_lowercase()))
    }

    /// Returns the next token from the input stream.
    fn next_token(&mut self) -> Option<Token> {
        let c = self.input.peek().cloned()?;
        let token = match c {
            Self::EQUAL | Self::GREATER | Self::LESS | ',' | '{' | '}' => {
                self.input.next();
                Some(Token::Delimiter(c))
            } // delimiter
            '\'' => self.parse_string(),                    // string
            c if c.is_ascii_digit() => self.parse_number(), // number
            c if c.is_alphabetic() || c == '_' => self.parse_identifier_or_keyword(), //  identifier or keyword
            _ => {
                self.input.next()?;
                self.next_token()
            }
        };
        self.current_token = token.clone();
        token
    }

    fn match_delim(&self, ch: char) -> bool {
        matches!(self.current_token, Some(Token::Delimiter(d)) if d == ch)
    }

    fn eat_delim(&mut self, ch: char) -> Result<(), ParserError> {
        if !self.match_delim(ch) {
            return Err(ParserError::BadSyntax);
        }
        self.next_token();
        Ok(())
    }

    fn match_int_constant(&self) -> bool {
        matches!(self.current_token, Some(Token::IntConstant(_)))
    }

    fn eat_int_constant(&mut self) -> Result<i32, ParserError> {
        if !self.match_int_constant() {
            return Err(ParserError::BadSyntax);
        }
        let Some(Token::IntConstant(i)) = self.current_token else {
            return Err(ParserError::BadSyntax);
        };
        self.next_token();
        Ok(i)
    }

    fn match_string_constant(&self) -> bool {
        matches!(self.current_token, Some(Token::StringConstant(_)))
    }

    fn eat_string_constant(&mut self) -> Result<String, ParserError> {
        if !self.match_string_constant() {
            return Err(ParserError::BadSyntax);
        }
        let Some(Token::StringConstant(s)) = self.current_token.clone() else {
            return Err(ParserError::BadSyntax);
        };
        self.next_token();
        Ok(s)
    }

    fn match_identifier(&self) -> bool {
        matches!(self.current_token, Some(Token::Identifier(_)))
    }

    fn eat_identifier(&mut self) -> Result<String, ParserError> {
        if !self.match_identifier() {
            return Err(ParserError::BadSyntax);
        }
        let Some(Token::Identifier(id)) = self.current_token.clone() else {
            return Err(ParserError::BadSyntax);
        };
        self.next_token();
        Ok(id)
    }

    fn match_keyword(&self, keyword: &str) -> bool {
        matches!(&self.current_token, Some(Token::Keyword(token)) if token == keyword)
    }

    fn eat_keyword(&mut self, keyword: &str) -> Result<String, ParserError> {
        if !self.match_keyword(keyword) {
            return Err(ParserError::BadSyntax);
        }
        let Some(Token::Keyword(keyword)) = self.current_token.clone() else {
            return Err(ParserError::BadSyntax);
        };
        self.next_token();
        Ok(keyword)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Token {
    Keyword(String),
    Identifier(String),
    IntConstant(i32),
    StringConstant(String),
    Delimiter(char),
}

#[cfg(test)]
mod lexer_tests {
    use crate::parser::Token;

    use super::Lexer;

    #[test]
    fn lexer_test() {
        let sql = "select a, b from student where id = 3";
        let mut lexer = Lexer::new(sql);

        let expected_tokens = vec![
            Token::Keyword("select".to_string()),
            Token::Identifier("a".to_string()),
            Token::Delimiter(','),
            Token::Identifier("b".to_string()),
            Token::Keyword("from".to_string()),
            Token::Identifier("student".to_string()),
            Token::Keyword("where".to_string()),
            Token::Identifier("id".to_string()),
            Token::Delimiter('='),
            Token::IntConstant(3),
        ];

        let first_token = lexer.current_token.clone().unwrap();
        let received_tokens: Vec<Token> = std::iter::from_fn(|| lexer.next_token()).collect();
        let all_tokens: Vec<Token> = std::iter::once(first_token)
            .chain(received_tokens)
            .collect();

        assert_eq!(all_tokens, expected_tokens);
    }

    #[test]
    fn test_string_constants() {
        let sql = "select name from users where city = 'New York'";
        let mut lexer = Lexer::new(sql);

        let expected_tokens = vec![
            Token::Keyword("select".to_string()),
            Token::Identifier("name".to_string()),
            Token::Keyword("from".to_string()),
            Token::Identifier("users".to_string()),
            Token::Keyword("where".to_string()),
            Token::Identifier("city".to_string()),
            Token::Delimiter('='),
            Token::StringConstant("New York".to_string()),
        ];

        let first_token = lexer.current_token.clone().unwrap();
        let received_tokens: Vec<Token> = std::iter::from_fn(|| lexer.next_token()).collect();
        let all_tokens: Vec<Token> = std::iter::once(first_token)
            .chain(received_tokens)
            .collect();

        assert_eq!(all_tokens, expected_tokens);
    }

    #[test]
    fn test_case_insensitive_keywords() {
        let sql = "SELECT name FROM users WHERE city = 'New York'";
        let mut lexer = Lexer::new(sql);

        let expected_tokens = vec![
            Token::Keyword("select".to_string()),
            Token::Identifier("name".to_string()),
            Token::Keyword("from".to_string()),
            Token::Identifier("users".to_string()),
            Token::Keyword("where".to_string()),
            Token::Identifier("city".to_string()),
            Token::Delimiter('='),
            Token::StringConstant("New York".to_string()),
        ];

        let first_token = lexer.current_token.clone().unwrap();
        let received_tokens: Vec<Token> = std::iter::from_fn(|| lexer.next_token()).collect();
        let all_tokens: Vec<Token> = std::iter::once(first_token)
            .chain(received_tokens)
            .collect();

        assert_eq!(all_tokens, expected_tokens);
    }
}

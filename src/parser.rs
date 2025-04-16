use std::{iter::Peekable, str::Chars};

#[derive(Debug, PartialEq, Eq)]
pub enum Token {
    Keyword(String),
    Identifier(String),
    IntConstant(i32),
    StringConstant(String),
    Delimiter(char),
}

struct Lexer<'a> {
    input: Peekable<Chars<'a>>,
    keywords: Vec<&'static str>,
}

impl<'a> Lexer<'a> {
    fn new(string: &'a str) -> Self {
        let keywords = [
            "select", "from", "where", "and", "insert", "into", "values", "delete", "update",
            "set", "create", "table", "int", "varchar", "view", "as", "index", "on",
        ];
        Self {
            input: string.chars().peekable(),
            keywords: keywords.to_vec(),
        }
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
        if self.keywords.contains(&string.as_str()) {
            return Some(Token::Keyword(string));
        }
        Some(Token::Identifier(string))
    }

    fn next_token(&mut self) -> Option<Token> {
        let c = self.input.peek().cloned()?;
        match c {
            '=' | ',' | '{' | '}' => {
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
        }
    }
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

        let received_tokens: Vec<Token> = std::iter::from_fn(|| lexer.next_token()).collect();

        assert_eq!(received_tokens, expected_tokens);
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

        let received_tokens: Vec<Token> = std::iter::from_fn(|| lexer.next_token()).collect();

        assert_eq!(received_tokens, expected_tokens);
    }
}

use std::{error::Error, fmt::Display, iter::Peekable, str::Chars};

use crate::{ComparisonOp, Constant, Expression, Predicate, Schema, Term};

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

    /// Parse a list of fields from the SQL statement
    /// Each field is just an identifier
    fn field_list(&mut self) -> Result<Vec<String>, ParserError> {
        let mut list = Vec::new();
        list.push(self.lexer.eat_identifier()?);
        while self.lexer.match_delim(',') {
            self.lexer.eat_delim(',')?;
            list.push(self.lexer.eat_identifier()?);
        }
        Ok(list)
    }

    fn select_list(&mut self) -> Result<Vec<String>, ParserError> {
        self.field_list()
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

    fn constants(&mut self) -> Result<Vec<Constant>, ParserError> {
        let mut const_list = Vec::new();
        const_list.push(self.constant()?);
        while self.lexer.match_delim(',') {
            self.lexer.eat_delim(',')?;
            const_list.push(self.constant()?);
        }
        Ok(const_list)
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
        let select_list = self.field_list()?;
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

    fn update_command(&mut self) -> Result<SQLStatement, ParserError> {
        if self.lexer.match_keyword("insert") {
            return Ok(SQLStatement::InsertData(self.insert()?));
        } else if self.lexer.match_keyword("delete") {
            return Ok(SQLStatement::DeleteData(self.delete()?));
        } else if self.lexer.match_keyword("update") {
            return Ok(SQLStatement::ModifyData(self.modify()?));
        } else {
            return self.create();
        }
    }

    fn create(&mut self) -> Result<SQLStatement, ParserError> {
        self.lexer.eat_keyword("create")?;
        if self.lexer.match_keyword("table") {
            return Ok(SQLStatement::CreateTableData(self.create_table()?));
        } else if self.lexer.match_keyword("view") {
            self.lexer.match_keyword("view");
            return Ok(SQLStatement::CreateViewData(self.create_view()?));
        } else if self.lexer.match_keyword("index") {
            self.lexer.match_keyword("index");
            return Ok(SQLStatement::CreateIndexData(self.create_index()?));
        } else {
            return Err(ParserError::BadSyntax);
        }
    }

    fn field_def(&mut self) -> Result<Schema, ParserError> {
        let field_name = self.lexer.eat_identifier()?;
        let mut schema = Schema::new();
        if self.lexer.match_keyword("int") {
            self.lexer.eat_keyword("int")?;
            schema.add_int_field(&field_name);
        } else if self.lexer.match_keyword("varchar") {
            self.lexer.eat_keyword("varchar")?;
            self.lexer.eat_delim('(')?;
            let size = self.lexer.eat_int_constant()?;
            self.lexer.eat_delim(')')?;
            schema.add_string_field(&field_name, size as usize);
        } else {
            return Err(ParserError::BadSyntax);
        }
        Ok(schema)
    }

    fn field_defs(&mut self) -> Result<Schema, ParserError> {
        let mut schema = Schema::new();
        schema.add_all_from_schema(&self.field_def()?);
        while self.lexer.match_delim(',') {
            self.lexer.eat_delim(',')?;
            schema.add_all_from_schema(&self.field_def()?);
        }
        Ok(schema)
    }

    fn create_table(&mut self) -> Result<CreateTableData, ParserError> {
        self.lexer.eat_keyword("table")?;
        let table_name = self.lexer.eat_identifier()?;
        self.lexer.eat_delim('(')?;
        let field_defs = self.field_defs()?;
        self.lexer.eat_delim(')')?;
        Ok(CreateTableData::new(table_name, field_defs))
    }

    fn create_view(&mut self) -> Result<CreateViewData, ParserError> {
        self.lexer.eat_keyword("view")?;
        let view_name = self.lexer.eat_identifier()?;
        self.lexer.eat_keyword("as")?;
        let query_data = self.query()?;
        Ok(CreateViewData::new(view_name, query_data))
    }

    fn create_index(&mut self) -> Result<CreateIndexData, ParserError> {
        self.lexer.eat_keyword("index")?;
        let index_name = self.lexer.eat_identifier()?;
        self.lexer.eat_keyword("on")?;
        let table_name = self.lexer.eat_identifier()?;
        self.lexer.eat_delim('(')?;
        let field = self.lexer.eat_identifier()?;
        self.lexer.eat_delim(')')?;
        Ok(CreateIndexData::new(index_name, table_name, field))
    }

    fn insert(&mut self) -> Result<InsertData, ParserError> {
        self.lexer.eat_keyword("insert")?;
        self.lexer.eat_keyword("into")?;
        let table_name = self.lexer.eat_identifier()?;
        self.lexer.eat_delim('(')?;
        let field_list = self.field_list()?;
        self.lexer.eat_delim(')')?;
        self.lexer.eat_keyword("values")?;
        self.lexer.eat_delim('(')?;
        let constants = self.constants()?;
        self.lexer.eat_delim(')')?;
        Ok(InsertData::new(table_name, field_list, constants))
    }

    fn delete(&mut self) -> Result<DeleteData, ParserError> {
        self.lexer.eat_keyword("delete")?;
        self.lexer.eat_keyword("from")?;
        let table_name = self.lexer.eat_identifier()?;
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
        Ok(DeleteData::new(table_name, predicate))
    }

    fn modify(&mut self) -> Result<ModifyData, ParserError> {
        self.lexer.eat_keyword("update")?;
        let table_name = self.lexer.eat_identifier()?;
        self.lexer.eat_keyword("set")?;
        let field_name = self.lexer.eat_identifier()?;
        self.lexer.eat_delim('=')?;
        let new_value = self.constant()?;
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
        Ok(ModifyData::new(
            table_name, field_name, new_value, predicate,
        ))
    }
}

#[cfg(test)]
mod parser_tests {
    use crate::{BooleanConnective, ComparisonOp, Constant, Expression, PredicateNode, Term};

    use super::{Parser, SQLStatement};

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

    #[test]
    fn test_create_table() {
        let sql = "CREATE TABLE students (id int, name varchar(20), age int)";
        let mut parser = Parser::new(sql);
        let stmt = parser.update_command().unwrap();

        if let SQLStatement::CreateTableData(create_table) = stmt {
            assert_eq!(create_table.table_name, "students");
            assert!(create_table.schema.fields.contains(&"id".to_string()));
            assert!(create_table.schema.fields.contains(&"name".to_string()));
            assert!(create_table.schema.fields.contains(&"age".to_string()));
        } else {
            panic!("Expected CreateTableData");
        }
    }

    #[test]
    fn test_insert() {
        let sql = "INSERT INTO users (name, age) VALUES ('Alice', 25)";
        let mut parser = Parser::new(sql);
        let stmt = parser.update_command().unwrap();

        if let SQLStatement::InsertData(insert) = stmt {
            assert_eq!(insert.table_name, "users");
            assert_eq!(insert.fields, vec!["name", "age"]);
            assert_eq!(
                insert.values,
                vec![Constant::String("Alice".to_string()), Constant::Int(25)]
            );
        } else {
            panic!("Expected InsertData");
        }
    }

    #[test]
    fn test_delete() {
        let sql = "DELETE FROM users WHERE age > 30";
        let mut parser = Parser::new(sql);
        let stmt = parser.update_command().unwrap();

        if let SQLStatement::DeleteData(delete) = stmt {
            assert_eq!(delete.table_name, "users");
            if let PredicateNode::Term(term) = &delete.predicate.root {
                assert!(matches!(term.lhs, Expression::FieldName(ref name) if name == "age"));
                assert!(matches!(term.rhs, Expression::Constant(Constant::Int(30))));
                assert!(matches!(term.comparison_op, ComparisonOp::GreaterThan));
            } else {
                panic!("Expected Term PredicateNode");
            }
        } else {
            panic!("Expected DeleteData");
        }
    }

    #[test]
    fn test_update() {
        let sql = "UPDATE employees SET salary = 50000 WHERE department = 'IT'";
        let mut parser = Parser::new(sql);
        let stmt = parser.update_command().unwrap();

        if let SQLStatement::ModifyData(modify) = stmt {
            assert_eq!(modify.table_name, "employees");
            assert_eq!(modify.field_name, "salary");
            assert_eq!(modify.new_value, Constant::Int(50000));
            if let PredicateNode::Term(term) = &modify.predicate.root {
                assert!(
                    matches!(term.lhs, Expression::FieldName(ref name) if name == "department")
                );
                assert!(
                    matches!(term.rhs, Expression::Constant(Constant::String(ref s)) if s == "IT")
                );
                assert!(matches!(term.comparison_op, ComparisonOp::Equal));
            } else {
                panic!("Expected Term PredicateNode");
            }
        } else {
            panic!("Expected ModifyData");
        }
    }

    #[test]
    fn test_create_index() {
        let sql = "CREATE INDEX idx_name ON users (name)";
        let mut parser = Parser::new(sql);
        let stmt = parser.update_command().unwrap();

        if let SQLStatement::CreateIndexData(create_index) = stmt {
            assert_eq!(create_index.index_name, "idx_name");
            assert_eq!(create_index.table_name, "users");
            assert_eq!(create_index.field_name, "name");
        } else {
            panic!("Expected CreateIndexData");
        }
    }

    #[test]
    fn test_create_view() {
        let sql =
            "CREATE VIEW high_salary AS SELECT name, salary FROM employees WHERE salary > 100000";
        let mut parser = Parser::new(sql);
        let stmt = parser.update_command().unwrap();

        if let SQLStatement::CreateViewData(create_view) = stmt {
            assert_eq!(create_view.view_name, "high_salary");
            assert_eq!(create_view.query_data.fields, vec!["name", "salary"]);
            assert_eq!(create_view.query_data.tables, vec!["employees"]);

            if let PredicateNode::Term(term) = &create_view.query_data.predicate.root {
                assert!(matches!(term.lhs, Expression::FieldName(ref name) if name == "salary"));
                assert!(matches!(
                    term.rhs,
                    Expression::Constant(Constant::Int(100000))
                ));
                assert!(matches!(term.comparison_op, ComparisonOp::GreaterThan));
            } else {
                panic!("Expected Term PredicateNode");
            }
        } else {
            panic!("Expected CreateViewData");
        }
    }

    #[test]
    fn test_complex_select() {
        let sql =
            "SELECT name, department FROM employees WHERE salary > 50000 AND department = 'IT'";
        let mut parser = Parser::new(sql);
        let query = parser.query().unwrap();

        assert_eq!(query.fields, vec!["name", "department"]);
        assert_eq!(query.tables, vec!["employees"]);

        if let PredicateNode::Composite { op, operands } = &query.predicate.root {
            assert!(matches!(op, BooleanConnective::And));
            assert_eq!(operands.len(), 2);

            if let PredicateNode::Term(term) = &operands[0] {
                assert!(matches!(term.lhs, Expression::FieldName(ref name) if name == "salary"));
                assert!(matches!(
                    term.rhs,
                    Expression::Constant(Constant::Int(50000))
                ));
                assert!(matches!(term.comparison_op, ComparisonOp::GreaterThan));
            }

            if let PredicateNode::Term(term) = &operands[1] {
                assert!(
                    matches!(term.lhs, Expression::FieldName(ref name) if name == "department")
                );
                assert!(
                    matches!(term.rhs, Expression::Constant(Constant::String(ref s)) if s == "IT")
                );
                assert!(matches!(term.comparison_op, ComparisonOp::Equal));
            }
        } else {
            panic!("Expected Composite PredicateNode");
        }
    }
}

#[derive(Debug)]
enum SQLStatement {
    CreateTableData(CreateTableData),
    CreateViewData(CreateViewData),
    CreateIndexData(CreateIndexData),
    InsertData(InsertData),
    DeleteData(DeleteData),
    ModifyData(ModifyData),
}

#[derive(Debug)]
struct ModifyData {
    table_name: String,
    field_name: String,
    new_value: Constant,
    predicate: Predicate,
}

impl ModifyData {
    fn new(
        table_name: String,
        field_name: String,
        new_value: Constant,
        predicate: Predicate,
    ) -> Self {
        Self {
            table_name,
            field_name,
            new_value,
            predicate,
        }
    }
}

#[derive(Debug)]
struct DeleteData {
    table_name: String,
    predicate: Predicate,
}

impl DeleteData {
    fn new(table_name: String, predicate: Predicate) -> Self {
        Self {
            table_name,
            predicate,
        }
    }
}

#[derive(Debug)]
struct InsertData {
    table_name: String,
    fields: Vec<String>,
    values: Vec<Constant>,
}

impl InsertData {
    fn new(table_name: String, fields: Vec<String>, values: Vec<Constant>) -> Self {
        Self {
            table_name,
            fields,
            values,
        }
    }
}

#[derive(Debug)]
struct CreateTableData {
    table_name: String,
    schema: Schema,
}

impl CreateTableData {
    fn new(table_name: String, schema: Schema) -> Self {
        Self { table_name, schema }
    }
}

#[derive(Debug)]
struct CreateViewData {
    view_name: String,
    query_data: QueryData,
}

impl CreateViewData {
    fn new(view_name: String, query_data: QueryData) -> Self {
        Self {
            view_name,
            query_data,
        }
    }
}

#[derive(Debug)]
struct CreateIndexData {
    index_name: String,
    table_name: String,
    field_name: String,
}

impl CreateIndexData {
    fn new(index_name: String, table_name: String, field_name: String) -> Self {
        Self {
            index_name,
            table_name,
            field_name,
        }
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
    const COMMA: char = ',';
    const ROUND_OPEN: char = '(';
    const ROUND_CLOSE: char = ')';
    const CURLY_OPEN: char = '{';
    const CURLY_CLOSE: char = '}';

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
            Self::EQUAL
            | Self::GREATER
            | Self::LESS
            | Self::COMMA
            | Self::ROUND_OPEN
            | Self::ROUND_CLOSE
            | Self::CURLY_OPEN
            | Self::CURLY_CLOSE => {
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

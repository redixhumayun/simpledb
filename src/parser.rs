
use std::{error::Error, fmt::Display, iter::Peekable, str::Chars};

use crate::{ComparisonOp, Constant, Expression, Predicate, Schema, Term};

#[derive(Debug)]
pub enum ParserError {
    BadSyntax,
    Other(Box<dyn Error>),
}

impl Error for ParserError {}

impl Display for ParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParserError::BadSyntax => write!(f, "Bad syntax"),
            ParserError::Other(err) => write!(f, "{}", err),
        }
    }
}

pub struct Parser<'a> {
    lexer: Lexer<'a>,
}

impl<'a> Parser<'a> {
    /// Creates a new Parser with the given SQL string
    pub fn new(string: &'a str) -> Self {
        Self {
            lexer: Lexer::new(string),
        }
    }

    /// Parses a comma-separated list of field names
    /// Returns: Vec<String> containing field names
    fn field_list(&mut self) -> Result<Vec<String>, ParserError> {
        let mut list = Vec::new();
        list.push(self.lexer.eat_identifier()?);
        while self.lexer.match_delim(',') {
            self.lexer.eat_delim(',')?;
            list.push(self.lexer.eat_identifier()?);
        }
        Ok(list)
    }

    /// Parses the SELECT clause field list
    /// Returns: Vec<String> containing selected field names
    fn select_list(&mut self) -> Result<Vec<String>, ParserError> {
        if self.lexer.match_delim('*') {
            self.lexer.eat_delim('*')?;
            return Ok(vec!["*".to_string()]);
        }
        self.field_list()
    }

    /// Parses the FROM clause table list
    /// Returns: Vec<String> containing table names
    fn select_tables(&mut self) -> Result<Vec<String>, ParserError> {
        let mut list = Vec::new();
        list.push(self.lexer.eat_identifier()?);
        while self.lexer.match_delim(',') {
            self.lexer.eat_delim(',')?;
            list.push(self.lexer.eat_identifier()?);
        }
        Ok(list)
    }

    /// Parses a constant value (string or integer)
    /// Returns: Constant enum variant
    fn constant(&mut self) -> Result<Constant, ParserError> {
        if self.lexer.match_string_constant() {
            return Ok(Constant::String(self.lexer.eat_string_constant()?));
        }
        Ok(Constant::Int(self.lexer.eat_int_constant()?))
    }

    /// Parses a comma-separated list of constants
    /// Returns: Vec<Constant> containing parsed values
    fn constants(&mut self) -> Result<Vec<Constant>, ParserError> {
        let mut const_list = Vec::new();
        const_list.push(self.constant()?);
        while self.lexer.match_delim(',') {
            self.lexer.eat_delim(',')?;
            const_list.push(self.constant()?);
        }
        Ok(const_list)
    }

    /// Parses an expression (field name or constant)
    /// Returns: Expression enum variant
    fn expression(&mut self) -> Result<Expression, ParserError> {
        if self.lexer.match_identifier() {
            return Ok(Expression::FieldName(self.lexer.eat_identifier()?));
        }
        Ok(Expression::Constant(self.constant()?))
    }

    /// Parses a term (comparison between expressions)
    /// Returns: Term struct containing lhs, rhs, and operator
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

    /// Parses multiple terms connected by AND
    /// Returns: Vec<Term> containing all parsed terms
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

    /// Parses a complete SELECT query
    /// Returns: QueryData containing fields, tables, and predicates
    pub fn query(&mut self) -> Result<QueryData, ParserError> {
        self.lexer.eat_keyword("select")?;
        let select_list = self.select_list()?;
        self.lexer.eat_keyword("from")?;
        let table_list = self.select_tables()?;
        let predicate = {
            if self.lexer.match_keyword("where") {
                self.lexer.eat_keyword("where")?;
                self.parse_predicate()?
            } else {
                Predicate::new(Vec::new())
            }
        };
        Ok(QueryData::new(select_list, table_list, predicate))
    }

    /// Parses any SQL command that modifies the database
    /// Returns: SQLStatement enum variant
    pub fn update_command(&mut self) -> Result<SQLStatement, ParserError> {
        if self.lexer.match_keyword("insert") {
            Ok(SQLStatement::Insert(self.insert()?))
        } else if self.lexer.match_keyword("delete") {
            return Ok(SQLStatement::Delete(self.delete()?));
        } else if self.lexer.match_keyword("update") {
            return Ok(SQLStatement::Modify(self.modify()?));
        } else {
            self.create()
        }
    }

    /// Parses CREATE TABLE/VIEW/INDEX statements
    /// Returns: SQLStatement enum variant
    fn create(&mut self) -> Result<SQLStatement, ParserError> {
        self.lexer.eat_keyword("create")?;
        if self.lexer.match_keyword("table") {
            Ok(SQLStatement::CreateTable(self.create_table()?))
        } else if self.lexer.match_keyword("view") {
            self.lexer.match_keyword("view");
            return Ok(SQLStatement::CreateView(self.create_view()?));
        } else if self.lexer.match_keyword("index") {
            self.lexer.match_keyword("index");
            return Ok(SQLStatement::CreateIndex(self.create_index()?));
        } else {
            return Err(ParserError::BadSyntax);
        }
    }

    /// Parses a single field definition (name and type)
    /// Returns: Schema containing the field definition
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

    /// Parses multiple field definitions
    /// Returns: Schema containing all field definitions
    fn field_defs(&mut self) -> Result<Schema, ParserError> {
        let mut schema = Schema::new();
        schema
            .add_all_from_schema(&self.field_def()?)
            .map_err(ParserError::Other)?;
        while self.lexer.match_delim(',') {
            self.lexer.eat_delim(',')?;
            schema
                .add_all_from_schema(&self.field_def()?)
                .map_err(ParserError::Other)?;
        }
        Ok(schema)
    }

    /// Parses CREATE TABLE statement
    /// Returns: CreateTableData containing table name and schema
    fn create_table(&mut self) -> Result<CreateTableData, ParserError> {
        self.lexer.eat_keyword("table")?;
        let table_name = self.lexer.eat_identifier()?;
        self.lexer.eat_delim('(')?;
        let field_defs = self.field_defs()?;
        self.lexer.eat_delim(')')?;
        Ok(CreateTableData::new(table_name, field_defs))
    }

    /// Parses CREATE VIEW statement
    /// Returns: CreateViewData containing view name and query
    fn create_view(&mut self) -> Result<CreateViewData, ParserError> {
        self.lexer.eat_keyword("view")?;
        let view_name = self.lexer.eat_identifier()?;
        self.lexer.eat_keyword("as")?;
        let query_data = self.query()?;
        Ok(CreateViewData::new(view_name, query_data))
    }

    /// Parses CREATE INDEX statement
    /// Returns: CreateIndexData containing index details
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

    /// Parses INSERT statement
    /// Returns: InsertData containing table name, fields, and values
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

    /// Parses DELETE statement
    /// Returns: DeleteData containing table name and predicate
    fn delete(&mut self) -> Result<DeleteData, ParserError> {
        self.lexer.eat_keyword("delete")?;
        self.lexer.eat_keyword("from")?;
        let table_name = self.lexer.eat_identifier()?;
        let predicate = {
            if self.lexer.match_keyword("where") {
                self.lexer.eat_keyword("where")?;
                self.parse_predicate()?
            } else {
                Predicate::new(Vec::new())
            }
        };
        Ok(DeleteData::new(table_name, predicate))
    }

    /// Parses UPDATE statement
    /// Returns: ModifyData containing update details
    fn modify(&mut self) -> Result<ModifyData, ParserError> {
        self.lexer.eat_keyword("update")?;
        let table_name = self.lexer.eat_identifier()?;
        self.lexer.eat_keyword("set")?;
        let field_name = self.lexer.eat_identifier()?;
        self.lexer.eat_delim('=')?;
        let new_value = self.expression()?;
        let predicate = {
            if self.lexer.match_keyword("where") {
                self.lexer.eat_keyword("where")?;
                self.parse_predicate()?
            } else {
                Predicate::new(Vec::new())
            }
        };
        Ok(ModifyData::new(
            table_name, field_name, new_value, predicate,
        ))
    }

    /// Parses a full predicate with proper precedence: NOT > AND > OR.
    fn parse_predicate(&mut self) -> Result<Predicate, ParserError> {
        self.parse_or()
    }

    /// Parses OR-chains: and-expr (OR and-expr)*
    fn parse_or(&mut self) -> Result<Predicate, ParserError> {
        let mut operands: Vec<Predicate> = Vec::new();
        operands.push(self.parse_and()?);
        while self.lexer.match_keyword("or") {
            self.lexer.eat_keyword("or")?;
            operands.push(self.parse_and()?);
        }
        if operands.len() == 1 {
            return Ok(operands.remove(0));
        }
        Ok(Predicate::or(operands))
    }

    /// Parses AND-chains: not-expr (AND not-expr)*
    fn parse_and(&mut self) -> Result<Predicate, ParserError> {
        let mut operands: Vec<Predicate> = Vec::new();
        operands.push(self.parse_not()?);
        while self.lexer.match_keyword("and") {
            self.lexer.eat_keyword("and")?;
            operands.push(self.parse_not()?);
        }
        if operands.len() == 1 {
            return Ok(operands.remove(0));
        }
        Ok(Predicate::and(operands))
    }

    /// Parses NOT: (NOT)* primary
    fn parse_not(&mut self) -> Result<Predicate, ParserError> {
        if self.lexer.match_keyword("not") {
            self.lexer.eat_keyword("not")?;
            let inner = self.parse_not()?;
            return Ok(Predicate::not(inner));
        }
        self.parse_primary_predicate()
    }

    /// Parses a parenthesized predicate or a single comparison term
    fn parse_primary_predicate(&mut self) -> Result<Predicate, ParserError> {
        if self.lexer.match_delim('(') {
            self.lexer.eat_delim('(')?;
            let pred = self.parse_predicate()?;
            self.lexer.eat_delim(')')?;
            return Ok(pred);
        }
        // Fallback to a single term
        let t = self.term()?;
        Ok(Predicate::new(vec![t]))
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
    fn parse_or_precedence() {
        let sql = "select b from t where a = 2 or a = 3 and c = 4";
        let mut parser = super::Parser::new(sql);
        let qd = parser.query().unwrap();
        // Expect Or at root
        match &qd.predicate.root {
            PredicateNode::Composite { op, operands } => {
                assert!(matches!(op, BooleanConnective::Or));
                assert_eq!(operands.len(), 2);
            }
            _ => panic!("expected composite"),
        }
    }

    #[test]
    fn parse_parentheses_and_not() {
        let sql = "select a from t where not (a = 1 and (b = 2 or c = 3))";
        let mut parser = super::Parser::new(sql);
        let qd = parser.query().unwrap();
        // Root should be NOT
        if let PredicateNode::Composite { op, operands } = &qd.predicate.root {
            assert!(matches!(op, BooleanConnective::Not));
            assert_eq!(operands.len(), 1);
        } else {
            panic!("expected NOT composite");
        }
    }

    #[test]
    fn test_create_table() {
        let sql = "CREATE TABLE students (id int, name varchar(20), age int)";
        let mut parser = Parser::new(sql);
        let stmt = parser.update_command().unwrap();

        if let SQLStatement::CreateTable(create_table) = stmt {
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

        if let SQLStatement::Insert(insert) = stmt {
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

        if let SQLStatement::Delete(delete) = stmt {
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

        if let SQLStatement::Modify(modify) = stmt {
            assert_eq!(modify.table_name, "employees");
            assert_eq!(modify.field_name, "salary");
            assert_eq!(modify.new_value, Expression::Constant(Constant::Int(50000)));
            // assert_eq!(modify.new_value, Constant::Int(50000));
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

        if let SQLStatement::CreateIndex(create_index) = stmt {
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

        if let SQLStatement::CreateView(create_view) = stmt {
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
pub enum SQLStatement {
    CreateTable(CreateTableData),
    CreateView(CreateViewData),
    CreateIndex(CreateIndexData),
    Insert(InsertData),
    Delete(DeleteData),
    Modify(ModifyData),
}

#[derive(Debug)]
pub struct ModifyData {
    pub table_name: String,
    pub field_name: String,
    pub new_value: Expression,
    pub predicate: Predicate,
}

impl ModifyData {
    fn new(
        table_name: String,
        field_name: String,
        new_value: Expression,
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
pub struct DeleteData {
    pub table_name: String,
    pub predicate: Predicate,
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
pub struct InsertData {
    pub table_name: String,
    pub fields: Vec<String>,
    pub values: Vec<Constant>,
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
pub struct CreateTableData {
    pub table_name: String,
    pub schema: Schema,
}

impl CreateTableData {
    fn new(table_name: String, schema: Schema) -> Self {
        Self { table_name, schema }
    }
}

#[derive(Debug)]
pub struct CreateViewData {
    pub view_name: String,
    pub query_data: QueryData,
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
pub struct CreateIndexData {
    pub index_name: String,
    pub table_name: String,
    pub field_name: String,
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
pub struct QueryData {
    pub fields: Vec<String>,
    pub tables: Vec<String>,
    pub predicate: Predicate,
}

impl QueryData {
    fn new(fields: Vec<String>, tables: Vec<String>, predicate: Predicate) -> Self {
        Self {
            fields,
            tables,
            predicate,
        }
    }

    pub fn to_sql(&self) -> String {
        let mut sql = String::from("SELECT ");
        sql.push_str(&self.fields.join(", "));
        sql.push_str("FROM ");
        sql.push_str(&self.tables.join(", "));

        todo!()
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
    const STAR: char = '*';

    /// Creates a new Lexer with the given SQL string
    fn new(string: &'a str) -> Self {
        let keywords = [
            "select", "from", "where", "and", "or", "not", "insert", "into", "values", "delete",
            "update", "set", "create", "table", "int", "varchar", "view", "as", "index", "on",
        ];
        let mut lexer = Self {
            input: string.chars().peekable(),
            keywords: keywords.iter().map(|s| s.to_lowercase()).collect(),
            current_token: None,
        };
        lexer.next_token().unwrap();
        lexer
    }

    /// Parses a string literal enclosed in single quotes
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

    /// Parses a numeric literal
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

    /// Parses an identifier or keyword
    fn parse_identifier_or_keyword(&mut self) -> Option<Token> {
        let mut string = String::new();
        while let Some(&c) = self.input.peek() {
            if !c.is_alphanumeric() && c != '_' {
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
            | Self::CURLY_CLOSE
            | Self::STAR => {
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

    /// Checks if current token matches the given delimiter
    fn match_delim(&self, ch: char) -> bool {
        matches!(self.current_token, Some(Token::Delimiter(d)) if d == ch)
    }

    /// Consumes the current token if it matches the given delimiter
    fn eat_delim(&mut self, ch: char) -> Result<(), ParserError> {
        if !self.match_delim(ch) {
            return Err(ParserError::BadSyntax);
        }
        self.next_token();
        Ok(())
    }

    /// Checks if current token is an integer constant
    fn match_int_constant(&self) -> bool {
        matches!(self.current_token, Some(Token::IntConstant(_)))
    }

    /// Consumes and returns the current integer constant
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

    /// Checks if current token is a string constant
    fn match_string_constant(&self) -> bool {
        matches!(self.current_token, Some(Token::StringConstant(_)))
    }

    /// Consumes and returns the current string constant
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

    /// Checks if current token is an identifier
    fn match_identifier(&self) -> bool {
        matches!(self.current_token, Some(Token::Identifier(_)))
    }

    /// Consumes and returns the current identifier
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

    /// Checks if current token matches the given keyword
    fn match_keyword(&self, keyword: &str) -> bool {
        matches!(&self.current_token, Some(Token::Keyword(token)) if token == keyword)
    }

    /// Consumes and returns the current keyword if it matches
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

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
            ParserError::Other(err) => write!(f, "{err}"),
        }
    }
}

pub struct Parser<'a> {
    lexer: Lexer<'a>,
}

impl<'a> Parser<'a> {
    pub fn new(string: &'a str) -> Self {
        Self {
            lexer: Lexer::new(string),
        }
    }

    fn field_list(&mut self) -> Result<Vec<String>, ParserError> {
        let mut list = Vec::new();
        list.push(self.lexer.eat_identifier()?);
        while self.lexer.match_delim(',') {
            self.lexer.eat_delim(',')?;
            list.push(self.lexer.eat_identifier()?);
        }
        Ok(list)
    }

    /// Aggregate function names (min, max, sum, count) are plain identifiers,
    /// not reserved keywords, so they can still be used as column names elsewhere.
    fn is_aggregate_fn(&self) -> bool {
        self.lexer.match_identifier_value("min")
            || self.lexer.match_identifier_value("max")
            || self.lexer.match_identifier_value("sum")
            || self.lexer.match_identifier_value("count")
    }

    /// `DISTINCT` is consumed as an identifier rather than a keyword so that
    /// `count` is not forced to claim it as a reserved word, which would break
    /// queries that use `distinct` as a column name outside aggregate context.
    fn parse_aggregate(&mut self) -> Result<AggregateSpec, ParserError> {
        if self.lexer.match_identifier_value("count") {
            self.lexer.eat_identifier()?;
            self.lexer.eat_delim('(')?;
            // "distinct" is not a keyword — consume it as an identifier.
            let kw = self.lexer.eat_identifier()?;
            if kw != "distinct" {
                return Err(ParserError::BadSyntax);
            }
            let field = self.lexer.eat_identifier()?;
            self.lexer.eat_delim(')')?;
            let alias = format!("count_distinct_{field}");
            return Ok(AggregateSpec {
                op: AggregateOp::CountDistinct,
                field,
                alias,
            });
        }

        let (op, prefix) = if self.lexer.match_identifier_value("min") {
            self.lexer.eat_identifier()?;
            (AggregateOp::Min, "min")
        } else if self.lexer.match_identifier_value("max") {
            self.lexer.eat_identifier()?;
            (AggregateOp::Max, "max")
        } else if self.lexer.match_identifier_value("sum") {
            self.lexer.eat_identifier()?;
            (AggregateOp::Sum, "sum")
        } else {
            return Err(ParserError::BadSyntax);
        };

        self.lexer.eat_delim('(')?;
        let field = self.lexer.eat_identifier()?;
        self.lexer.eat_delim(')')?;
        let alias = format!("{prefix}_{field}");
        Ok(AggregateSpec { op, field, alias })
    }

    /// Aggregate aliases (e.g. `max_price`) are inserted into the field list so
    /// `ProjectPlan` can reference them by name without special-casing aggregates.
    fn select_list(&mut self) -> Result<(Vec<String>, Vec<AggregateSpec>), ParserError> {
        if self.lexer.match_delim('*') {
            self.lexer.eat_delim('*')?;
            return Ok((vec!["*".to_string()], vec![]));
        }

        let mut fields = Vec::new();
        let mut aggregates = Vec::new();

        loop {
            if self.is_aggregate_fn() {
                let spec = self.parse_aggregate()?;
                fields.push(spec.alias.clone());
                aggregates.push(spec);
            } else {
                fields.push(self.lexer.eat_identifier()?);
            }
            if !self.lexer.match_delim(',') {
                break;
            }
            self.lexer.eat_delim(',')?;
        }

        Ok((fields, aggregates))
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

    /// Float is tried before int so that `3.14` produces `Float(3.14)` rather
    /// than `Int(3)` followed by a stray `.14` that would fail the next token.
    fn constant(&mut self) -> Result<Constant, ParserError> {
        if self.lexer.match_string_constant() {
            return Ok(Constant::String(self.lexer.eat_string_constant()?));
        }
        let negative = self.lexer.match_delim('-');
        if negative {
            self.lexer.eat_delim('-')?;
        }
        if self.lexer.match_float_constant() {
            let v = self.lexer.eat_float_constant()?;
            return Ok(Constant::Float(if negative { -v } else { v }));
        }
        let v = self.lexer.eat_int_constant()?;
        Ok(Constant::Int(if negative { -v } else { v }))
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

    /// Identifiers are checked first: in a WHERE clause `age > 30`, `age` is a
    /// field name and `30` is a constant, so the identifier branch must win before
    /// falling through to constant parsing.
    fn expression(&mut self) -> Result<Expression, ParserError> {
        if self.lexer.match_identifier() {
            return Ok(Expression::FieldName(self.lexer.eat_identifier()?));
        }
        Ok(Expression::Constant(self.constant()?))
    }

    /// The operator is matched directly on `current_token` (not via `eat_*`),
    /// so `next_token()` must be called explicitly before parsing the RHS.
    fn term(&mut self) -> Result<Term, ParserError> {
        let lhs = self.expression()?;
        let op = match self.lexer.current_token {
            Some(Token::Delimiter(Lexer::EQUAL)) => ComparisonOp::Equal,
            Some(Token::Delimiter(Lexer::GREATER)) => ComparisonOp::GreaterThan,
            Some(Token::Delimiter(Lexer::LESS)) => ComparisonOp::LessThan,
            Some(Token::LessOrEqual) => ComparisonOp::LessThanOrEqual,
            Some(Token::GreaterOrEqual) => ComparisonOp::GreaterThanOrEqual,
            Some(Token::NotEqual) => ComparisonOp::NotEqual,
            _ => return Err(ParserError::BadSyntax),
        };
        self.lexer
            .next_token()
            .ok_or_else(|| ParserError::BadSyntax)?;
        let rhs = self.expression()?;
        Ok(Term::new_with_op(lhs, rhs, op))
    }

    /// Dead code — predicate parsing was rewritten to use `parse_predicate`.
    fn _terms(&mut self) -> Result<Vec<Term>, ParserError> {
        let mut terms = Vec::new();
        terms.push(self.term()?);
        //  TODO: Handle more boolean connectives
        while self.lexer.match_keyword("and") {
            self.lexer.eat_keyword("and")?;
            terms.push(self.term()?);
        }
        Ok(terms)
    }

    pub fn query(&mut self) -> Result<QueryData, ParserError> {
        self.lexer.eat_keyword("select")?;
        let (select_fields, aggregates) = self.select_list()?;
        self.lexer.eat_keyword("from")?;
        let table_list = self.select_tables()?;
        let predicate = if self.lexer.match_keyword("where") {
            self.lexer.eat_keyword("where")?;
            self.parse_predicate()?
        } else {
            Predicate::new(Vec::new())
        };
        let order_by = if self.lexer.match_keyword("order") {
            self.lexer.eat_keyword("order")?;
            self.lexer.eat_keyword("by")?;
            self.parse_order_by_list()?
        } else {
            Vec::new()
        };
        Ok(QueryData::new(
            select_fields,
            table_list,
            predicate,
            order_by,
            aggregates,
        ))
    }

    fn parse_order_by_list(&mut self) -> Result<Vec<(String, SortDirection)>, ParserError> {
        let mut list = Vec::new();
        loop {
            let field = self.lexer.eat_identifier()?;
            let dir = if self.lexer.match_keyword("desc") {
                self.lexer.eat_keyword("desc")?;
                SortDirection::Desc
            } else {
                if self.lexer.match_keyword("asc") {
                    self.lexer.eat_keyword("asc")?;
                }
                SortDirection::Asc
            };
            list.push((field, dir));
            if !self.lexer.match_delim(',') {
                break;
            }
            self.lexer.eat_delim(',')?;
        }
        Ok(list)
    }

    /// CREATE has no explicit branch here because `create()` consumes the
    /// `create` keyword itself, unlike INSERT/DELETE/UPDATE which are matched
    /// above and then dispatched. Anything that isn't insert/delete/update
    /// falls through and either succeeds as CREATE or returns BadSyntax.
    pub fn update_command(&mut self) -> Result<SQLStatement, ParserError> {
        if self.lexer.match_keyword("insert") {
            Ok(SQLStatement::Insert(self.insert()?))
        } else if self.lexer.match_keyword("delete") {
            Ok(SQLStatement::Delete(self.delete()?))
        } else if self.lexer.match_keyword("update") {
            Ok(SQLStatement::Modify(self.modify()?))
        } else {
            self.create()
        }
    }

    /// The `self.lexer.match_keyword("view")` and `match_keyword("index")` calls
    /// in the view/index branches are no-ops — their return value is discarded.
    /// The keyword is left unconsumed here and eaten by `create_view`/`create_index`.
    fn create(&mut self) -> Result<SQLStatement, ParserError> {
        self.lexer.eat_keyword("create")?;
        if self.lexer.match_keyword("table") {
            Ok(SQLStatement::CreateTable(self.create_table()?))
        } else if self.lexer.match_keyword("view") {
            self.lexer.match_keyword("view");
            Ok(SQLStatement::CreateView(self.create_view()?))
        } else if self.lexer.match_keyword("index") {
            self.lexer.match_keyword("index");
            Ok(SQLStatement::CreateIndex(self.create_index()?))
        } else {
            Err(ParserError::BadSyntax)
        }
    }

    /// `DECIMAL(p,s)` and `NUMERIC(p,s)` are accepted as aliases for `FLOAT`.
    /// Precision and scale are parsed and discarded — the engine stores all
    /// floating-point values as f64 regardless of declared precision.
    fn field_def(&mut self) -> Result<Schema, ParserError> {
        let field_name = self.lexer.eat_identifier()?;
        let mut schema = Schema::new();
        if self.lexer.match_keyword("int") {
            self.lexer.eat_keyword("int")?;
            schema.add_int_field(&field_name);
        } else if self.lexer.match_keyword("float")
            || self.lexer.match_keyword("decimal")
            || self.lexer.match_keyword("numeric")
        {
            self.lexer.next_token();
            // Optional precision/scale: DECIMAL(p) or DECIMAL(p,s) — consume and ignore.
            if self.lexer.match_delim('(') {
                self.lexer.eat_delim('(')?;
                self.lexer.eat_int_constant()?;
                if self.lexer.match_delim(',') {
                    self.lexer.eat_delim(',')?;
                    self.lexer.eat_int_constant()?;
                }
                self.lexer.eat_delim(')')?;
            }
            schema.add_float_field(&field_name);
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

    /// Only single-column indexes are supported; the grammar accepts exactly one field name.
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

    /// Field count and value count are not validated to match; a mismatch
    /// produces a silently malformed `InsertData` that will fail later at execution.
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

    /// WHERE is optional; omitting it produces an empty predicate that matches
    /// every row, so the executor will delete the entire table.
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

    /// WHERE is optional; omitting it produces an empty predicate that matches
    /// every row, so the executor will update the entire table.
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

    /// Precedence: NOT binds tightest, then AND, then OR — standard SQL.
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
            PredicateNode::Composite { op: _, operands: _ }
        );
        let PredicateNode::Composite { op: _, operands } = &query_data.predicate.root else {
            panic!("Expected Composite PredicateNode");
        };
        matches!(
            &operands[0],
            PredicateNode::Term(Term {
                lhs: Expression::FieldName(_id),
                rhs: Expression::Constant(Constant::Int(val)),
                comparison_op: ComparisonOp::Equal,
            }) if *val == 3
        );
        matches!(
            &operands[1],
            PredicateNode::Term(Term {
                lhs: Expression::FieldName(_),
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

    #[test]
    fn test_float_constant_tokenization() {
        let sql = "SELECT price FROM products WHERE price > 9.99";
        let mut parser = Parser::new(sql);
        let query = parser.query().unwrap();

        if let PredicateNode::Term(term) = &query.predicate.root {
            assert!(matches!(term.lhs, Expression::FieldName(ref name) if name == "price"));
            assert!(matches!(
                term.rhs,
                Expression::Constant(Constant::Float(v)) if (v - 9.99).abs() < 1e-10
            ));
            assert!(matches!(term.comparison_op, ComparisonOp::GreaterThan));
        } else {
            panic!("Expected Term PredicateNode");
        }
    }

    #[test]
    fn test_negative_float_literal() {
        let sql = "INSERT INTO t (x) VALUES (-3.14)";
        let mut parser = Parser::new(sql);
        let stmt = parser.update_command().unwrap();

        if let SQLStatement::Insert(insert) = stmt {
            assert_eq!(insert.values.len(), 1);
            assert!(matches!(
                insert.values[0],
                Constant::Float(v) if (v - (-3.14)).abs() < 1e-10
            ));
        } else {
            panic!("Expected InsertData");
        }
    }

    #[test]
    fn test_create_table_decimal_types() {
        // DECIMAL(p,s) and NUMERIC(p,s) are aliases for float
        let sql = "CREATE TABLE orders (id int, amount decimal(10,2), price numeric(8,4))";
        let mut parser = Parser::new(sql);
        let stmt = parser.update_command().unwrap();

        if let SQLStatement::CreateTable(create_table) = stmt {
            assert_eq!(create_table.table_name, "orders");
            use crate::FieldType;
            let schema = &create_table.schema;
            assert_eq!(schema.info["amount"].field_type, FieldType::Float);
            assert_eq!(schema.info["price"].field_type, FieldType::Float);
        } else {
            panic!("Expected CreateTableData");
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateOp {
    Min,
    Max,
    Sum,
    CountDistinct,
}

/// One aggregate expression from a SELECT list, e.g. `MAX(o_id)`.
#[derive(Debug, Clone)]
pub struct AggregateSpec {
    pub op: AggregateOp,
    /// Input column name.
    pub field: String,
    /// Output column name in the result schema, e.g. `max_o_id`.
    pub alias: String,
}

#[derive(Debug)]
pub struct QueryData {
    pub fields: Vec<String>,
    pub tables: Vec<String>,
    pub predicate: Predicate,
    pub order_by: Vec<(String, SortDirection)>,
    pub aggregates: Vec<AggregateSpec>,
}

impl QueryData {
    fn new(
        fields: Vec<String>,
        tables: Vec<String>,
        predicate: Predicate,
        order_by: Vec<(String, SortDirection)>,
        aggregates: Vec<AggregateSpec>,
    ) -> Self {
        Self {
            fields,
            tables,
            predicate,
            order_by,
            aggregates,
        }
    }

    /// Not implemented — always panics. Exists as a placeholder.
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
    const BANG: char = '!';
    const PLUS: char = '+';
    const MINUS: char = '-';
    const SLASH: char = '/';
    const PERCENT: char = '%';

    /// `next_token()` is called at the end to prime `current_token`. Without it
    /// every `match_*` call would return false because `current_token` starts as `None`.
    fn new(string: &'a str) -> Self {
        let keywords = [
            "select", "from", "where", "and", "or", "not", "insert", "into", "values", "delete",
            "update", "set", "create", "table", "int", "varchar", "float", "decimal", "numeric",
            "view", "as", "index", "on", "order", "by", "asc", "desc",
        ];
        let mut lexer = Self {
            input: string.chars().peekable(),
            keywords: keywords.iter().map(|s| s.to_lowercase()).collect(),
            current_token: None,
        };
        lexer.next_token().unwrap();
        lexer
    }

    /// No escape sequence support: a literal `'` inside a string cannot be
    /// represented. `''` (SQL standard doubling) is not handled either.
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

    /// Once a `.` is seen after digits, the token is committed to Float —
    /// a trailing dot (e.g. `3.`) therefore produces `FloatConstant(3.0)`.
    fn parse_number(&mut self) -> Option<Token> {
        let mut number = String::new();
        while let Some(&c) = self.input.peek() {
            if !c.is_ascii_digit() {
                break;
            }
            number.push(c);
            self.input.next();
        }
        if self.input.peek() == Some(&'.') {
            number.push('.');
            self.input.next();
            while let Some(&c) = self.input.peek() {
                if !c.is_ascii_digit() {
                    break;
                }
                number.push(c);
                self.input.next();
            }
            return Some(Token::FloatConstant(number.parse().unwrap()));
        }
        Some(Token::IntConstant(number.parse().unwrap()))
    }

    /// All identifiers and keywords are lowercased, making the lexer case-insensitive.
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

    /// Whitespace and unrecognised characters are skipped by recursing — the
    /// `_` arm consumes one character and calls `next_token` again rather than
    /// returning an error, so the lexer is tolerant of unknown input.
    fn next_token(&mut self) -> Option<Token> {
        let c = self.input.peek().cloned()?;
        let token = match c {
            // Check for multi-character operators
            Self::LESS => {
                self.input.next();
                if self.input.peek() == Some(&Self::EQUAL) {
                    self.input.next();
                    Some(Token::LessOrEqual)
                } else {
                    Some(Token::Delimiter(Self::LESS))
                }
            }
            Self::GREATER => {
                self.input.next();
                if self.input.peek() == Some(&Self::EQUAL) {
                    self.input.next();
                    Some(Token::GreaterOrEqual)
                } else {
                    Some(Token::Delimiter(Self::GREATER))
                }
            }
            Self::BANG => {
                self.input.next();
                if self.input.peek() == Some(&Self::EQUAL) {
                    self.input.next();
                    Some(Token::NotEqual)
                } else {
                    // Standalone ! is not valid in SQL, skip it
                    self.next_token()
                }
            }
            // Single-character delimiters
            Self::EQUAL
            | Self::COMMA
            | Self::ROUND_OPEN
            | Self::ROUND_CLOSE
            | Self::CURLY_OPEN
            | Self::CURLY_CLOSE
            | Self::STAR
            | Self::PLUS
            | Self::MINUS
            | Self::SLASH
            | Self::PERCENT => {
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

    fn match_float_constant(&self) -> bool {
        matches!(self.current_token, Some(Token::FloatConstant(_)))
    }

    fn eat_float_constant(&mut self) -> Result<f64, ParserError> {
        if !self.match_float_constant() {
            return Err(ParserError::BadSyntax);
        }
        let Some(Token::FloatConstant(f)) = self.current_token else {
            return Err(ParserError::BadSyntax);
        };
        self.next_token();
        Ok(f)
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

    fn match_identifier_value(&self, value: &str) -> bool {
        matches!(&self.current_token, Some(Token::Identifier(id)) if id == value)
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

#[derive(Clone, Debug)]
pub enum Token {
    Keyword(String),
    Identifier(String),
    IntConstant(i32),
    FloatConstant(f64),
    StringConstant(String),
    Delimiter(char),
    // Multi-character operators
    LessOrEqual,
    GreaterOrEqual,
    NotEqual,
}

/// `FloatConstant` uses `to_bits()` equality for the same reason as `Constant::PartialEq`:
/// f64 doesn't implement `Eq`, so the derived impl is unavailable.
impl PartialEq for Token {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Token::Keyword(a), Token::Keyword(b)) => a == b,
            (Token::Identifier(a), Token::Identifier(b)) => a == b,
            (Token::IntConstant(a), Token::IntConstant(b)) => a == b,
            (Token::FloatConstant(a), Token::FloatConstant(b)) => a.to_bits() == b.to_bits(),
            (Token::StringConstant(a), Token::StringConstant(b)) => a == b,
            (Token::Delimiter(a), Token::Delimiter(b)) => a == b,
            (Token::LessOrEqual, Token::LessOrEqual) => true,
            (Token::GreaterOrEqual, Token::GreaterOrEqual) => true,
            (Token::NotEqual, Token::NotEqual) => true,
            _ => false,
        }
    }
}

impl Eq for Token {}

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

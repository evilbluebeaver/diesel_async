use diesel::result::Error;
use diesel::QueryResult;
use scoped_futures::ScopedBoxFuture;
use std::borrow::Cow;

use crate::AsyncConnection;

// TODO: refactor this to share more code with diesel

pub struct TransactionToken<
    'token,
    C: AsyncConnection<TransactionManager = TM>,
    TM: TransactionManager<C>,
> {
    conn: &'token mut C,
    done: bool,
}

impl<'token, C: AsyncConnection<TransactionManager = TM>, TM: TransactionManager<C>>
    TransactionToken<'token, C, TM>
{
    fn new(conn: &'token mut C) -> Self {
        Self { conn, done: false }
    }
    fn complete(&mut self) {
        self.done = true;
    }
}

impl<'token, C: AsyncConnection<TransactionManager = TM>, TM: TransactionManager<C>> Drop
    for TransactionToken<'token, C, TM>
{
    fn drop(&mut self) {
        if self.done {
            // println!("Nothing to do here");
            return;
        }
        TM::rollback_transaction_no_return(self.conn);
    }
}

// impl<'token, C: AsyncConnection> TransactionToken<'token, C> for AnsiTransactionToken<'token, C> {}

/// Manages the internal transaction state for a connection.
///
/// You will not need to interact with this trait, unless you are writing an
/// implementation of [`AsyncConnection`].
#[async_trait::async_trait]
pub trait TransactionManager<Conn: AsyncConnection>: Send {
    /// Data stored as part of the connection implementation
    /// to track the current transaction state of a connection
    type TransactionStateData;

    /// Begin a new transaction
    ///
    async fn begin_transaction(conn: &mut Conn) -> QueryResult<()>;

    /// Rollback the transaction
    ///
    async fn rollback_transaction(conn: &mut Conn) -> QueryResult<()>;

    /// Rollback the transaction without returning a result
    ///
    fn rollback_transaction_no_return(conn: &mut Conn);

    /// Commit the transaction
    ///
    async fn commit_transaction(token: &mut Conn) -> QueryResult<()>;

    /// Fetch the current transaction status as mutable
    ///
    /// Used to ensure that `begin_test_transaction` is not called when already
    /// inside of a transaction, and that operations are not run in a `InError`
    /// transaction manager.
    #[doc(hidden)]
    fn transaction_manager_status_mut(conn: &mut Conn) -> &mut TransactionManagerStatus;

    /// Executes the given function inside of a database transaction
    ///
    /// Each implementation of this function needs to fulfill the documented
    /// behaviour of [`AsyncConnection::transaction`]
    async fn transaction<'a, F, R, E>(conn: &mut Conn, callback: F) -> Result<R, E>
    where
        F: for<'r> FnOnce(&'r mut Conn) -> ScopedBoxFuture<'a, 'r, Result<R, E>> + Send + 'a,
        E: From<Error> + Send,
        R: Send,
    {
        let mut token = TransactionToken::new(conn);
        Self::begin_transaction(token.conn).await?;
        match callback(token.conn).await {
            Ok(value) => {
                Self::commit_transaction(token.conn).await?;
                token.complete();
                Ok(value)
            }
            Err(user_error) => match Self::rollback_transaction(token.conn).await {
                Ok(()) => Err(user_error),
                Err(Error::BrokenTransactionManager) => {
                    // In this case we are probably more interested by the
                    // original error, which likely caused this
                    Err(user_error)
                }
                Err(rollback_error) => Err(rollback_error.into()),
            },
        }
    }

    /// This methods checks if the connection manager is considered to be broken
    /// by connection pool implementations
    ///
    /// A connection manager is considered to be broken by default if it either
    /// contains an open transaction (because you don't want to have connections
    /// with open transactions in your pool) or when the transaction manager is
    /// in an error state.
    #[doc(hidden)]
    fn is_broken_transaction_manager(conn: &mut Conn) -> bool {
        match Self::transaction_manager_status_mut(conn).transaction_state() {
            // all transactions are closed
            // so we don't consider this connection broken
            Ok(ValidTransactionManagerStatus {
                in_transaction: None,
            }) => false,
            // The transaction manager is in an error state
            // Therefore we consider this connection broken
            Err(_) => true,
            // The transaction manager contains a open transaction
            // we do consider this connection broken
            // if that transaction was not opened by `begin_test_transaction`
            Ok(ValidTransactionManagerStatus {
                in_transaction: Some(s),
            }) => !s.test_transaction,
        }
    }
}

/// An implementation of `TransactionManager`
#[derive(Default, Debug)]
pub struct AnsiTransactionManager {
    pub(crate) status: TransactionManagerStatus,
}

/// Status of the transaction manager
#[derive(Debug)]
pub enum TransactionManagerStatus {
    /// Valid status, the manager can run operations
    Valid(ValidTransactionManagerStatus),
    /// Error status, probably following a broken connection. The manager will no longer run operations
    InError,
}

impl Default for TransactionManagerStatus {
    fn default() -> Self {
        TransactionManagerStatus::Valid(ValidTransactionManagerStatus::default())
    }
}

impl TransactionManagerStatus {
    /// Sets the transaction manager status to InError
    ///
    /// Subsequent attempts to use transaction-related features will result in a
    /// [`Error::BrokenTransactionManager`] error
    pub fn set_in_error(&mut self) {
        *self = TransactionManagerStatus::InError
    }

    fn transaction_state(&mut self) -> QueryResult<&mut ValidTransactionManagerStatus> {
        match self {
            TransactionManagerStatus::Valid(valid_status) => Ok(valid_status),
            TransactionManagerStatus::InError => Err(Error::BrokenTransactionManager),
        }
    }

    pub(crate) fn set_test_transaction_flag(&mut self) {
        if let TransactionManagerStatus::Valid(ValidTransactionManagerStatus {
            in_transaction: Some(s),
        }) = self
        {
            s.test_transaction = true;
        }
    }
}

/// Valid transaction status for the manager
#[allow(missing_copy_implementations)]
#[derive(Debug, Default)]
pub struct ValidTransactionManagerStatus {
    in_transaction: Option<InTransactionStatus>,
}

#[allow(missing_copy_implementations)]
#[derive(Debug)]
struct InTransactionStatus {
    test_transaction: bool,
}

impl ValidTransactionManagerStatus {
    /// Return the current transaction status
    ///
    pub fn in_transaction(&self) -> bool {
        self.in_transaction.is_some()
    }

    pub fn start_transaction(&mut self) {
        self.in_transaction = Some(InTransactionStatus {
            test_transaction: false,
        });
    }

    pub fn stop_transaction(&mut self) {
        self.in_transaction = None;
    }
}

impl AnsiTransactionManager {
    fn get_transaction_state<Conn>(
        conn: &mut Conn,
    ) -> QueryResult<&mut ValidTransactionManagerStatus>
    where
        Conn: AsyncConnection<TransactionManager = Self>,
    {
        conn.transaction_state().status.transaction_state()
    }

    /// Begin a transaction with custom SQL
    ///
    /// This is used by connections to implement more complex transaction APIs
    /// to set things such as isolation levels.
    /// Returns an error if already inside of a transaction.
    pub async fn begin_transaction_sql<Conn>(conn: &mut Conn, sql: &str) -> QueryResult<()>
    where
        Conn: AsyncConnection<TransactionManager = Self>,
    {
        let state = Self::get_transaction_state(conn)?;
        match state.in_transaction() {
            false => {
                conn.batch_execute(sql).await?;
                Self::get_transaction_state(conn)?.start_transaction();
                Ok(())
            }
            true => Err(Error::AlreadyInTransaction),
        }
    }
}

#[async_trait::async_trait]
impl<Conn> TransactionManager<Conn> for AnsiTransactionManager
where
    Conn: AsyncConnection<TransactionManager = Self>,
{
    type TransactionStateData = Self;

    async fn begin_transaction(conn: &mut Conn) -> QueryResult<()> {
        let transaction_state = Self::get_transaction_state(conn)?;
        match transaction_state.in_transaction() {
            false => {
                let start_transaction_sql = Cow::from("BEGIN");
                conn.batch_execute(&start_transaction_sql).await?;
                Self::get_transaction_state(conn)?.start_transaction();
                Ok(())
            }
            true => Err(Error::AlreadyInTransaction),
        }
    }

    async fn rollback_transaction(conn: &mut Conn) -> QueryResult<()> {
        let transaction_state = Self::get_transaction_state(conn)?;
        let rollback_sql = match transaction_state.in_transaction() {
            false => return Err(Error::NotInTransaction),
            true => Cow::Borrowed("ROLLBACK"),
        };
        match conn.batch_execute(&rollback_sql).await {
            Ok(()) => {
                Self::get_transaction_state(conn)?.stop_transaction();
                Ok(())
            }
            Err(rollback_error) => {
                Self::transaction_manager_status_mut(conn).set_in_error();
                Err(rollback_error)
            }
        }
    }

    fn rollback_transaction_no_return(conn: &mut Conn) {
        println!("rollback_transaction_no_return");
        Self::get_transaction_state(conn)
            .expect("Transaction state")
            .stop_transaction();
        conn.simple_query_send("ROLLBACK")
    }

    /// If the transaction fails to commit due to a `SerializationFailure` or a
    /// `ReadOnlyTransaction` a rollback will be attempted. If the rollback succeeds,
    /// the original error will be returned, otherwise the error generated by the rollback
    /// will be returned. In the second case the connection will be considered broken
    /// as it contains a uncommitted unabortable open transaction.
    async fn commit_transaction(conn: &mut Conn) -> QueryResult<()> {
        let transaction_state = Self::get_transaction_state(conn)?;
        let commit_sql = match transaction_state.in_transaction() {
            false => return Err(Error::NotInTransaction),
            true => Cow::Borrowed("COMMIT"),
        };
        match conn.batch_execute(&commit_sql).await {
            Ok(()) => {
                Self::get_transaction_state(conn)?.stop_transaction();
                Ok(())
            }
            Err(commit_error) => match Self::rollback_transaction(conn).await {
                Ok(()) => Err(commit_error),
                Err(rollback_error) => {
                    conn.transaction_state().status.set_in_error();
                    Err(Error::RollbackErrorOnCommit {
                        rollback_error: Box::new(rollback_error),
                        commit_error: Box::new(commit_error),
                    })
                }
            },
        }
    }

    fn transaction_manager_status_mut(conn: &mut Conn) -> &mut TransactionManagerStatus {
        &mut conn.transaction_state().status
    }
}

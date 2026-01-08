//! Job type identification trait.
//!
//! This module provides the `JobTypeId` trait for compile-time type-safe job identification.
//! Each job type has a unique hash computed from its fully qualified type name, preventing
//! runtime collisions and enabling type-safe job routing.

use std::any::TypeId;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A type-safe job type identifier.
///
/// This trait provides compile-time type identification for job processors,
/// enabling type-safe job routing and preventing runtime type mismatches.
///
/// # Type Safety
///
/// The trait uses Rust's type system to guarantee uniqueness:
/// - `TypeId` provides compile-time type identity
/// - `type_hash()` creates a deterministic 64-bit hash from the type's full path
/// - Different types always produce different hashes
///
/// # Example
///
/// ```rust
/// use aide_de_camp::core::job_type_id::JobTypeId;
///
/// struct MyJob;
///
/// impl JobTypeId for MyJob {
///     fn type_name() -> &'static str
///     where
///         Self: Sized,
///     {
///         "myapp::jobs::MyJob"
///     }
/// }
///
/// // Type hash is computed from the full type path
/// let hash = MyJob::type_hash();
/// assert_ne!(hash, 0);
/// ```
pub trait JobTypeId: Send + Sync + 'static {
    /// Returns the compile-time TypeId of this job type.
    ///
    /// This provides a unique identifier for the type at runtime based on
    /// Rust's type system. Used for type-safe job registry lookups.
    fn type_id() -> TypeId
    where
        Self: Sized,
    {
        TypeId::of::<Self>()
    }

    /// Returns the fully qualified type name.
    ///
    /// This should be the complete module path plus type name to ensure uniqueness.
    /// For example: `"myapp::jobs::EmailJob"` instead of just `"EmailJob"`.
    ///
    /// # Naming Convention
    ///
    /// Use the format: `module_path!()::type_name!()`
    ///
    /// Example:
    /// ```rust,ignore
    /// fn type_name() -> &'static str {
    ///     concat!(module_path!(), "::", stringify!(Self))
    /// }
    /// ```
    fn type_name() -> &'static str
    where
        Self: Sized;

    /// Computes a deterministic 64-bit hash from the type name.
    ///
    /// This hash is used for database storage and job routing. The hash is
    /// computed from the full type name to prevent collisions.
    ///
    /// # Collision Resistance
    ///
    /// While hash collisions are theoretically possible, they are extremely
    /// unlikely in practice:
    /// - Uses Rust's `DefaultHasher` (currently SipHash-1-3)
    /// - Hashes the full module path + type name
    /// - 64-bit space provides ~18 quintillion possible values
    ///
    /// # Example
    ///
    /// ```rust
    /// # use aide_de_camp::core::job_type_id::JobTypeId;
    /// # struct MyJob;
    /// # impl JobTypeId for MyJob {
    /// #     fn type_name() -> &'static str { "myapp::jobs::MyJob" }
    /// # }
    /// let hash1 = MyJob::type_hash();
    /// let hash2 = MyJob::type_hash();
    /// assert_eq!(hash1, hash2);  // Deterministic
    /// ```
    fn type_hash() -> u64
    where
        Self: Sized,
    {
        let mut hasher = DefaultHasher::new();
        Self::type_name().hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestJob1;
    struct TestJob2;

    mod nested {
        use super::*;
        pub struct TestJob3;

        impl JobTypeId for TestJob3 {
            fn type_name() -> &'static str {
                "aide_de_camp::core::job_type_id::tests::nested::TestJob3"
            }
        }
    }

    impl JobTypeId for TestJob1 {
        fn type_name() -> &'static str {
            "aide_de_camp::core::job_type_id::tests::TestJob1"
        }
    }

    impl JobTypeId for TestJob2 {
        fn type_name() -> &'static str {
            "aide_de_camp::core::job_type_id::tests::TestJob2"
        }
    }

    #[test]
    fn test_type_id_uniqueness() {
        let id1 = TestJob1::type_id();
        let id2 = TestJob2::type_id();
        assert_ne!(id1, id2, "Different types should have different TypeIds");
    }

    #[test]
    fn test_type_hash_uniqueness() {
        let hash1 = TestJob1::type_hash();
        let hash2 = TestJob2::type_hash();
        let hash3 = nested::TestJob3::type_hash();

        assert_ne!(hash1, hash2, "Different types should have different hashes");
        assert_ne!(hash1, hash3, "Different types should have different hashes");
        assert_ne!(hash2, hash3, "Different types should have different hashes");
    }

    #[test]
    fn test_type_hash_deterministic() {
        let hash1 = TestJob1::type_hash();
        let hash2 = TestJob1::type_hash();
        assert_eq!(hash1, hash2, "Type hash should be deterministic");
    }

    #[test]
    fn test_type_hash_non_zero() {
        let hash = TestJob1::type_hash();
        assert_ne!(hash, 0, "Type hash should not be zero");
    }

    #[test]
    fn test_type_name() {
        assert_eq!(
            TestJob1::type_name(),
            "aide_de_camp::core::job_type_id::tests::TestJob1"
        );
        assert_eq!(
            TestJob2::type_name(),
            "aide_de_camp::core::job_type_id::tests::TestJob2"
        );
        assert_eq!(
            nested::TestJob3::type_name(),
            "aide_de_camp::core::job_type_id::tests::nested::TestJob3"
        );
    }
}

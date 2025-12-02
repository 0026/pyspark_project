from project_pyspark.common.helper import is_datasets_valid
    
def test_validate_returns_true_when_all_rows_match(spark):
    """
    Test that `is_datasets_valid` returns True when all datasets are fully valid
    and all cross-references match correctly.

    Scenario:
    - `calls_per_area_info` contains two unique caller IDs (1 and 2), with no
      duplicates and no missing records.
    - `sales_info` references caller_id values 1 and 2, both of which are
      present in `calls_per_area_info`, so all joins should match.
    - `personal_and_sales_info` contains two correctly formatted address entries.
      No address-formatting or structural issues are present.
    
    Because:
      * all IDs match,
      * no rows are duplicated,
      * no rows are missing,
      * and address data is valid,
    the dataset should pass validation and return True.

    This test verifies the “happy path” where all datasets are correct and
    consistent with one another.
    """
    calls_per_area_info = spark.createDataFrame(
        [
            (1,5,2), (2,5,2)
        ],
        ["id","calls_made", "calls_successful"]
    )
    sales_info = spark.createDataFrame(
        [
            (10, 1),   
            (11, 2)   
        ],
        ["id", "caller_id"]
    )
    personal_and_sales_info = spark.createDataFrame(
        [
            ("aaaa, 11, 1111 AA",),   
            ("bbbb, 11, 1111 BB",)   
        ],
        ["address"]
    )

    assert is_datasets_valid(sales_info, personal_and_sales_info, calls_per_area_info) 

def test_validate_calls_per_area_info_returns_false_when_unmached_rows_match(spark):
    """
    Test that `is_datasets_valid` returns False when `sales_info` contains
    caller IDs that do not exist in `calls_per_area_info`.

    Scenario:
    - `calls_per_area_info` contains only caller_id = 1 (duplicated), and does
      not contain caller_id = 2.
    - `sales_info` contains two rows:
        * caller_id = 1 → matches a record in calls_per_area_info
        * caller_id = 2 → does NOT match any record in calls_per_area_info
    - `personal_and_sales_info` contains valid address entries and is not the
      source of failure in this test.

    Since `sales_info` contains at least one unmatched caller_id (value 2),
    the validation logic should detect this mismatch and return False.

    This test explicitly checks the scenario where missing relationships between
    datasets cause validation to fail.
    """
    calls_per_area_info = spark.createDataFrame(
        [
            (1,5,2), (1,5,2)
        ],
        ["id","calls_made", "calls_successful"]
    )
    sales_info = spark.createDataFrame(
        [
            (10, 1),   
            (11, 2)   
        ],
        ["id", "caller_id"]
    )
    personal_and_sales_info = spark.createDataFrame(
        [
            ("aaaa, 11, 1111 AA",),   
            ("bbbb, 11, 1111 BB",)   
        ],
        ["address"]
    )
    assert is_datasets_valid(sales_info, personal_and_sales_info, calls_per_area_info) is False 

def test_validate_calls_per_area_info_returns_false_when_missing_rows_match(spark):
    """
    Test that `is_datasets_valid` returns False when `calls_per_area_info`
    is missing required rows needed to match all `sales_info.caller_id` values.

    Scenario:
    - `calls_per_area_info` contains only one record with id=1.
    - `sales_info` contains caller_id values 1 and 2.
        * caller_id = 1 → valid match
        * caller_id = 2 → missing in calls_per_area_info (unmatched)
    
    The validation should detect the unmatched sales row (caller_id=2) and
    return False.

    This test specifically verifies the behavior when the lookup table
    (`calls_per_area_info`) lacks the necessary IDs to satisfy all sales rows.
    """
    calls_per_area_info = spark.createDataFrame(
        [
            (1,5,2)
        ],
        ["id","calls_made", "calls_successful"]
    )
    sales_info = spark.createDataFrame(
        [
            (10, 1),   
            (11, 2)   
        ],
        ["id", "caller_id"]
    )
    personal_and_sales_info = spark.createDataFrame(
        [
            ("aaaa, 11, 1111 AA",),   
            ("bbbb, 11, 1111 BB",)   
        ],
        ["address"]
    )
    assert is_datasets_valid(sales_info, personal_and_sales_info, calls_per_area_info) is False 

def test_validate_calls_per_area_info_returns_false_when_wrong_values_calls_made_and_calls_successful(spark):
    """
    Test that `is_datasets_valid` returns False when `calls_per_area_info`
    contains incorrect values for `calls_made` or `calls_successful` that
    violate expected validation rules.

    Scenario:
    - `calls_per_area_info` contains two records:
        * ID=1 → calls_made = 1, calls_successful = 2 (calls_successful > calls_made, invalid)
        * ID=2 → calls_made = 5, calls_successful = 2 (valid)
    - `sales_info` references caller_id values 1 and 2, both present in
      calls_per_area_info.
    - `personal_and_sales_info` contains valid address entries and is not the
      source of failure.

    The validation function is expected to fail because the first row in
    `calls_per_area_info` contains inconsistent data (calls_successful > calls_made),
    which violates business rules.

    This test verifies that `is_datasets_valid` correctly detects invalid numeric
    values in the calls dataset.
    """
    calls_per_area_info = spark.createDataFrame(
        [
            (1,1,2), (2,5,2)
        ],
        ["id","calls_made", "calls_successful"]
    )
    sales_info = spark.createDataFrame(
        [
            (10, 1),   
            (11, 2)   
        ],
        ["id", "caller_id"]
    )
    personal_and_sales_info = spark.createDataFrame(
        [
            ("aaaa, 11, 1111 AA",),   
            ("bbbb, 11, 1111 BB",)   
        ],
        ["address"]
    )

    assert is_datasets_valid(sales_info, personal_and_sales_info, calls_per_area_info) is False

def test_validate_personal_and_sales_info_returns_false_when_address_is_incorrect(spark):
    """
    Test that `is_datasets_valid` returns False when at least one row in
    `personal_and_sales_info` contains an incorrectly formatted or invalid
    address.

    Scenario:
    - `calls_per_area_info` contains valid IDs (1 and 2).
    - `sales_info` correctly references caller_id values 1 and 2.
      → No issues with matching or duplicates.
    - `personal_and_sales_info` contains one correctly formatted address and
      one incorrectly formatted address:
        * "aaaa 11, 1111 AA" → missing a comma between the name and number,
          violating the expected address format.
        * "bbbb, 11, 1111 BB" → assumed to be valid.

    Because the validation logic should detect the incorrectly formatted
    address, the function is expected to return False.

    This test specifically verifies the part of dataset validation that checks
    address correctness and formatting.
    """
    calls_per_area_info = spark.createDataFrame(
        [
            (1,5,2), (2,5,2)
        ],
        ["id","calls_made", "calls_successful"]
    )
    sales_info = spark.createDataFrame(
        [
            (10, 1),   
            (11, 2)   
        ],
        ["id", "caller_id"]
    )
    personal_and_sales_info = spark.createDataFrame(
        [
            ("aaaa 11, 1111 AA",),   
            ("bbbb, 11, 1111 BB",)   
        ],
        ["address"]
    )

    assert is_datasets_valid(sales_info, personal_and_sales_info, calls_per_area_info) is False
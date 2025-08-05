# Pytest Testing Guide for SQLAlchemy Dialect Development

This guide covers pytest testing patterns and best practices for SQLAlchemy dialect development, based on the testing framework used in this project.

## Basic Test Structure

### Simple Fixture Example

```python
import pytest

@pytest.fixture
def my_fixture():
    return "test_value"

def test_with_fixture(my_fixture):
    assert my_fixture == "test_value"
```

### Database Connection Fixture

For SQLAlchemy dialect testing:

```python
import pytest
import sqlalchemy as sa

@pytest.fixture(scope="class")
def db_connection():
    url = os.environ['SINGLESTOREDB_URL']
    if not url.startswith('singlestoredb'):
        url = 'singlestoredb+' + url
    engine = sa.create_engine(url)
    conn = engine.connect()
    yield conn
    conn.close()
```

## Test Parametrization

### Basic Parametrization

```python
@pytest.mark.parametrize("test_input,expected", [("3+5", 8), ("2+4", 6), ("6*9", 42)])
def test_eval(test_input, expected):
    assert eval(test_input) == expected
```

### Class-Level Parametrization

```python
@pytest.mark.parametrize("n,expected", [(1, 2), (3, 4)])
class TestClass:
    def test_simple_case(self, n, expected):
        assert n + 1 == expected

    def test_weird_simple_case(self, n, expected):
        assert (n * 1) + 1 == expected
```

### Module-Level Parametrization

```python
import pytest

pytestmark = pytest.mark.parametrize("n,expected", [(1, 2), (3, 4)])

class TestClass:
    def test_simple_case(self, n, expected):
        assert n + 1 == expected
```

## Fixture Parametrization

### Parametrized Fixtures

```python
@pytest.fixture(scope="module", params=["smtp.gmail.com", "mail.python.org"])
def smtp_connection(request):
    smtp_connection = smtplib.SMTP(request.param, 587, timeout=5)
    yield smtp_connection
    print(f"finalizing {smtp_connection}")
    smtp_connection.close()
```

### Custom Test IDs for Parametrized Fixtures

```python
@pytest.fixture(params=[0, 1], ids=["spam", "ham"])
def data_fixture(request):
    return request.param

def test_data(data_fixture):
    pass

# Using a function to generate IDs
def idfn(fixture_value):
    if fixture_value == 0:
        return "zero"
    else:
        return None

@pytest.fixture(params=[0, 1], ids=idfn)
def another_fixture(request):
    return request.param
```

## Advanced Parametrization Techniques

### Indirect Parametrization

```python
@pytest.fixture
def db(request):
    if request.param == "mysql":
        return MySQL()
    elif request.param == "postgresql":
        return PostgreSQL()

@pytest.mark.parametrize("db", ["mysql", "postgresql"], indirect=True)
def test_database(db):
    assert db.is_connected()
```

### Partial Indirect Parametrization

```python
@pytest.fixture
def x(request):
    return request.param * 3

@pytest.fixture
def y(request):
    return request.param * 2

@pytest.mark.parametrize("x, y", [("a", "b")], indirect=["x"])
def test_indirect(x, y):
    assert x == "aaa"  # x processed by fixture
    assert y == "b"    # y used directly
```

## Custom Parametrization with pytest_generate_tests

### Dynamic Parametrization from Command Line

```python
# conftest.py
def pytest_addoption(parser):
    parser.addoption(
        "--stringinput",
        action="append",
        default=[],
        help="list of stringinputs to pass to test functions",
    )

def pytest_generate_tests(metafunc):
    if "stringinput" in metafunc.fixturenames:
        metafunc.parametrize("stringinput", metafunc.config.getoption("stringinput"))

# test_strings.py
def test_valid_string(stringinput):
    assert stringinput.isalpha()
```

Run with: `pytest --stringinput="hello" --stringinput="world"`

### Class-Based Parametrization

```python
def pytest_generate_tests(metafunc):
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(
        argnames, [[funcargs[name] for name in argnames] for funcargs in funcarglist]
    )

class TestClass:
    params = {
        "test_equals": [dict(a=1, b=2), dict(a=3, b=3)],
        "test_zerodivision": [dict(a=1, b=0)],
    }

    def test_equals(self, a, b):
        assert a == b

    def test_zerodivision(self, a, b):
        with pytest.raises(ZeroDivisionError):
            a / b
```

## Marks and Expected Failures

### Using pytest.param with Marks

```python
@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("3+5", 8),
        ("2+4", 6),
        pytest.param("6*9", 42, marks=pytest.mark.xfail)
    ],
)
def test_eval(test_input, expected):
    assert eval(test_input) == expected
```

### Fixture with Skip Mark

```python
@pytest.fixture(params=[0, 1, pytest.param(2, marks=pytest.mark.skip)])
def data_set(request):
    return request.param

def test_data(data_set):
    pass
```

## Integration with unittest.TestCase

### Using Fixtures with unittest

```python
# conftest.py
@pytest.fixture(scope="class")
def db_class(request):
    class DummyDB:
        pass
    request.cls.db = DummyDB()

# test_unittest_db.py
import unittest
import pytest

@pytest.mark.usefixtures("db_class")
class MyTest(unittest.TestCase):
    def test_method1(self):
        assert hasattr(self, "db")

    def test_method2(self):
        assert hasattr(self, "db")
```

### Autouse Fixtures in unittest

```python
class MyTest(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def initdir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        tmp_path.joinpath("samplefile.ini").write_text("# testdata")

    def test_method(self):
        with open("samplefile.ini") as f:
            s = f.read()
        assert "testdata" in s
```

## Fixture Scopes and Dependencies

### Different Scopes

```python
@pytest.fixture(scope="session")
def session_fixture():
    return "session_data"

@pytest.fixture(scope="module")
def module_fixture():
    return "module_data"

@pytest.fixture(scope="class")
def class_fixture():
    return "class_data"

@pytest.fixture(scope="function")  # default
def function_fixture():
    return "function_data"
```

### Fixture Dependencies

```python
@pytest.fixture
def order():
    return []

@pytest.fixture
def append_first(order):
    order.append(1)

@pytest.fixture
def append_second(order, append_first):
    order.extend([2])

@pytest.fixture(autouse=True)
def append_third(order, append_second):
    order += [3]

def test_order(order):
    assert order == [1, 2, 3]
```

## Built-in Fixtures

### Common Built-in Fixtures

- `tmp_path`: Provides a pathlib.Path object to a temporary directory
- `monkeypatch`: Temporarily modify classes, functions, dictionaries, os.environ
- `caplog`: Control logging and access log entries
- `capsys`: Capture output to sys.stdout and sys.stderr
- `request`: Provide information on the executing test function

### Using tmp_path

```python
def test_temp_file(tmp_path):
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")
    assert test_file.read_text() == "test content"
```

### Using monkeypatch

```python
def test_environment_variable(monkeypatch):
    monkeypatch.setenv("TEST_VAR", "test_value")
    assert os.environ["TEST_VAR"] == "test_value"
```

## Configuration

### pytest.ini Configuration

```ini
[pytest]
# Apply fixtures to all test functions
usefixtures =
    clean_db

# Disable test ID escaping for Unicode (use with caution)
disable_test_id_escaping_and_forfeit_all_rights_to_community_support = True
```

### Loading External Fixtures

```python
# conftest.py
pytest_plugins = "mylibrary.fixtures"
```

## Best Practices for SQLAlchemy Dialect Testing

1. **Use class-scoped fixtures** for database connections to reduce setup overhead
2. **Parametrize fixtures** to test against multiple database configurations
3. **Use indirect parametrization** for complex setup scenarios
4. **Leverage pytest marks** to skip tests based on database capabilities
5. **Use pytest_generate_tests** for dynamic test generation based on discovered database features
6. **Apply fixtures at module level** using `pytestmark` for consistent test setup
7. **Use proper cleanup** with yield fixtures or finalizers
8. **Test both success and failure scenarios** using pytest.raises and xfail marks

## Example SQLAlchemy Dialect Test Structure

```python
import pytest
import sqlalchemy as sa

class TestBasicDialect:
    @pytest.fixture(scope="class")
    def engine(self):
        url = os.environ['SINGLESTOREDB_URL']
        return sa.create_engine(url)

    @pytest.fixture
    def connection(self, engine):
        conn = engine.connect()
        yield conn
        conn.close()

    @pytest.mark.parametrize("sql,expected", [
        ("SELECT 1", [(1,)]),
        ("SELECT 'test'", [('test',)]),
    ])
    def test_basic_queries(self, connection, sql, expected):
        result = list(connection.exec_driver_sql(sql))
        assert result == expected

    def test_reflection(self, connection):
        inspector = sa.inspect(connection)
        tables = inspector.get_table_names()
        assert isinstance(tables, list)
```

# WiredTiger Testing

## Introduction

TBD

## Generate the test coverage documentation

To generate the test coverage documentation, execute the following command:

```bash
python test_tag.py [options]
```
> test_tag.py is located under the dist/ directory

The script parses all *.py and main.c files present in the test/ directory.

-h for help
-i to generate stats
-p to list all files that are missing a test

## How tag a file ?

Each test file shall contain a tag in the following format:

```
[TEST_TAGS]
<COMPONENT>:<TESTING_TYPE>:<TESTING_AREA>
[END_TAGS]
```

One test file can have multiple tags:

```
[TEST_TAGS]
backup:correctness:full_backup
checkpoints:correctness:checkpoint_data
caching_eviction:correctness:written_data
[END_TAGS]
```

If a test file shall be ignored, the following tag can be used:

```
[TEST_TAGS]
ignored_file
[END_TAGS]
```

## Components



## Usage

```python
import foobar

foobar.pluralize('word') # returns 'words'
foobar.pluralize('goose') # returns 'geese'
foobar.singularize('phenomena') # returns 'phenomenon'
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[Test Coverage](../output.md)
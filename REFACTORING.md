# Housing Analysis Refactoring Plan

## Current Issues

1. **Monolithic Design**: Single `Datasets` class with 20+ methods handling all data operations
2. **Hard-coded Paths**: File paths embedded in class (`../data/`)
3. **Code Duplication**: Repeated patterns across FRED and NY Fed data loaders
4. **No Configuration Management**: API keys and settings scattered in notebooks
5. **Missing Type Hints**: No type annotations for better IDE support and code clarity
6. **Unused Files**: `utils.py` exists but is empty

## Proposed Structure

```
housing_analysis/
├── __init__.py
├── config.py              # Centralized configuration
├── data/
│   ├── __init__.py
│   ├── base.py           # Abstract base classes for data loaders
│   ├── fred.py           # FRED API data operations
│   ├── newyorkfed.py     # NY Fed data operations  
│   ├── census.py         # Census data operations
│   └── loaders.py        # High-level unified interface
├── visualization/
│   ├── __init__.py
│   └── plots.py          # Reusable plotting utilities
├── utils.py              # General utilities & calculations
└── exceptions.py         # Custom exceptions
```

## Key Improvements

### 1. Data Source Separation
- Each data provider (FRED, NY Fed, Census) gets its own module
- Common interface through base classes
- Easier to maintain and extend

### 2. Configuration Management
- Centralized `config.py` for paths, API keys, and settings
- Environment variable support
- Configurable data directories

### 3. Reusable Components
- Extract `plot_recessions()` and other plotting helpers to `visualization/`
- Move mortgage calculations to `utils.py`
- Create data transformation utilities

### 4. Code Quality
- Add comprehensive type hints
- Implement proper error handling
- Add docstrings following numpy style
- Create custom exceptions for better debugging

## Implementation Steps

1. **Create new module structure**
   - Set up `data/` subpackage with `__init__.py` files
   - Create `visualization/` subpackage
   - Add `config.py` and `exceptions.py`

2. **Extract FRED operations**
   - Move all `load_*()` methods for FRED data to `data/fred.py`
   - Create `FREDDataLoader` class
   - Add data update functionality from `FRED_data_pull.ipynb`

3. **Extract NY Fed operations**
   - Move NY Fed methods to `data/newyorkfed.py`
   - Create `NYFedDataLoader` class
   - Consolidate date conversion and cleaning methods

4. **Create base classes**
   - Define `DataLoader` abstract base class in `data/base.py`
   - Establish common interface for all data sources
   - Add validation and caching mechanisms

5. **Build unified interface**
   - Create high-level `DataManager` in `data/loaders.py`
   - Provide simple API for notebooks
   - Maintain backward compatibility where possible

6. **Move utilities**
   - Extract `calculate_mortgage_payment()` to `utils.py`
   - Move data transformation functions
   - Add any general-purpose helpers

7. **Update notebooks**
   - Import from new module structure
   - Use configuration for paths
   - Leverage reusable plotting utilities

## Benefits

- **Maintainability**: Clear separation of concerns makes code easier to understand and modify
- **Testability**: Modular design enables unit testing of individual components
- **Extensibility**: Easy to add new data sources or functionality
- **Reusability**: Common utilities can be shared across notebooks
- **Type Safety**: Type hints improve IDE support and catch errors early

## Migration Notes

- The refactoring will be done incrementally to avoid breaking changes
- Existing notebooks will continue to work during the transition
- A compatibility layer can be provided if needed
- Documentation will be updated as modules are refactored
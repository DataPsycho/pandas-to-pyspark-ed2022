import pandas as pd
PANDAS_DISPLAY_SETTING = [
    ('display.max_rows', 500),
    ('display.max_columns', 500),
    ('display.width', 1000)
]


def set_pd_display(module: pd) -> bool:
    for item in PANDAS_DISPLAY_SETTING:
        module.set_option(*item)
    return True

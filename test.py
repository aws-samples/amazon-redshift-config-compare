import os


test_dict = {"param": "val"}


def get_db_params(dicts):
    if dicts.get("param") is None:
        master_user_name = "test"
    else:
        master_user_name = "test2"
    other_val = "other"
    return (master_user_name, other_val)


master_user_name, other_val = get_db_params(test_dict)


print(master_user_name)

from sqlalchemy import create_engine

def get_engine(engine_str:str):
    return create_engine(engine_str)

def write_txt(name, data):
    with open(name + '.txt', 'w') as f:
        for item in data:
            f.write(f"{item}\n")

def read_txt(file_path):
    with open(file_path, 'r') as f:
        my_list = [line.strip() for line in f]
    return my_list
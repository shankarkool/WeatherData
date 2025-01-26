import ast
import os

def find_local_imports(filepath):
    with open(filepath, 'r') as file:
        tree = ast.parse(file.read())

    local_imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for name in node.names:
                local_imports.append(name.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:  # local import
                local_imports.append(node.module)

    return local_imports

def draw_dependencies(path):
    dependencies = {}
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                local_imports = find_local_imports(filepath)
                dependencies[os.path.relpath(filepath, path)] = [os.path.relpath(os.path.join(root, imp.replace('.', os.sep) + '.py'), path) if '.' in imp else os.path.relpath(os.path.join(root, imp + '.py'), path) for imp in local_imports if imp + '.py' in files or imp.split('.')[0] in dirs]

    with open('dependencies.dot', 'w') as f:
        f.write("digraph G {\n")
        for file, imports in dependencies.items():
            for imp in imports:
                f.write(f'  "{file}" -> "{imp}";\n')
        f.write("}")

def main():
    path = input("Enter the path: ")
    draw_dependencies(path)

if __name__ == "__main__":
    main()

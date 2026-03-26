import pydot, os

def main():
    for dir_name in os.listdir('.'):
        # if it is a dir
        if os.path.isdir(dir_name):
            for file_name in os.listdir(dir_name):
                # if it is a dot file
                if file_name.endswith('color.dot'):
                    # convert it to png
                    dot_path = os.path.join(dir_name, file_name)
                    graph = pydot.graph_from_dot_file(dot_path)[0]
                    graph.write_png(os.path.join(dir_name, file_name[:-4] + '.png'))

if __name__ == '__main__':
    main()
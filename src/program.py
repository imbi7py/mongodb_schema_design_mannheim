import queries
import colorama

import colors


def main():
    colorama.init()

    print(colors.bold_color + "MongoDB Schema Performance app by @mkennedy")
    print(colors.subdue_color + 'https://github.com/mikeckennedy/mongodb_schema_design_mannheim')
    print()

    queries.run()


if __name__ == '__main__':
    main()

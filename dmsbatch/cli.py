from argparse import ArgumentParser
import dmsbatch
from dmsbatch import __version__
from dmsbatch import commands


def config_generate_cmd(args):
    if not args.file:
        print('Specify config file!')
        return
    print('Generating blank config: ', args.file)
    commands.generate_blank_config(args.file)

def cli(args=None):
    p = ArgumentParser(
        description="Azure Batch for Delta Modeling Section ",
        conflict_handler='resolve'
    )
    p.set_defaults(func=lambda args: p.print_help())
    p.add_argument(
        '-v', '--version',
        action='version',
        help='Show the conda-prefix-replacement version number and exit.',
        version="dmsbatch %s" % __version__,
    )

    # do something with the sub commands
    sub_p = p.add_subparsers(help='sub-command help')
    # add show all sensors command
    p1 = sub_p.add_parser('gen-config', help='generate config')
    p1.add_argument('-f', '--file', type=str, required=True, help='output file')
    p1.set_defaults(func=config_generate_cmd)
    # Now call the appropriate response.
    args = p.parse_args(args)
    args.func(args)
    return
    # No return value means no error.
    # Return a value of 1 or higher to signify an error.
    # See https://docs.python.org/3/library/sys.html#sys.exit


if __name__ == '__main__':
    import sys
    cli(sys.argv[1:])

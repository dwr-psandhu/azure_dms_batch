from argparse import ArgumentParser
from dmsbatch import __version__

def config_cmd(args):
    print('Called config with ', args)

def config_generate_cmd(args):
    print('Called config generate with ', args)

def cli(args=None):
    p = ArgumentParser(
        description="Azure Batch for Delta Modeling Section ",
        conflict_handler='resolve'
    )
    p.set_defaults(func=lambda args: p.print_help())
    p.add_argument(
        '-V', '--version',
        action='version',
        help='Show the conda-prefix-replacement version number and exit.',
        version="dmsbatch %s" % __version__,
    )

    # do something with the sub commands
    sub_p = p.add_subparsers(help='sub-command help')
    # add show all sensors command
    config_cmd = sub_p.add_parser('config', help='config related commands')
    config_cmd.set_defaults(func=config_cmd)
    config_generate_cmd = config_cmd.add_subparsers(help='config related commands')
    config_generate_cmd.add_parser('generate')
    config_generate_cmd.set_defaults(func=config_generate_cmd)
    # Now call the appropriate response.
    pargs = p.parse_args(args)
    pargs.func(pargs)
    return 
    # No return value means no error.
    # Return a value of 1 or higher to signify an error.
    # See https://docs.python.org/3/library/sys.html#sys.exit


if __name__ == '__main__':
    import sys
    cli(sys.argv[1:])

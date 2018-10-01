from hpc import hpc


if __name__ == '__main__':
    p = hpc('lsf')
    p.set_cmd("ls -l > files")
    p.submit()
    p.speak()

from invoke import task

@task
def build(c):
    c.run("python ftest.py")


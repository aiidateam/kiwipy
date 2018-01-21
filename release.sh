


version=`python -c 'import kiwi; print(kiwi.__version__)'`
tag="v${version}"
relbranch="release-${version}"

echo Releasing version $version

git checkout -b $relbranch 

git commit -m "Release ${version}"

git tag -a $tag -m "Version $version"


git checkout master
git merge $relbranch
git branch -d $relbranch

git push origin master
git push origin $tag

rm -r build
rm -r plumpy.egg-info
python setup.py sdist
python setup.py bdist_wheel --universal

twine upload dist/*


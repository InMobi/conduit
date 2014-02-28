#!/bin/sh

die() {
    echo "$1"
    exit 1
}

REPO=git@github.com:InMobi/conduit.git
TMP=/tmp/conduit-site-stage
STAGE=`pwd`/target/staging
VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version|grep -Ev '(^\[|Download\w+:)' || die "unable to get version")

mvn clean site site:stage || die "unable to generate site"

rm -rf $TMP || die "unable to clear $TMP"

git clone $REPO $TMP || die "unable to clone $TMP"
cd $TMP

git checkout gh-pages || die "unable to checkout gh-pages"

mkdir -p current || die "unable to create dir current"
mkdir -p versions/$VERSION || die "unable to create dir versions/$VERSION"

find current -type f -exec git rm {} \;

mkdir -p current || die "unable to create dir current"
mkdir -p versions/$VERSION || die "unable to create dir versions/$VERSION"

cp -r $STAGE/* current/ || die "unable to copy to current"
cp -r $STAGE/* versions/$VERSION/ || die "unable to copy to versions/$VERSION"

FILES=$(cd versions; ls -t | grep -v index.html)
echo '<ul>' > versions/index.html
for f in $FILES
do
    echo "<li><a href='$f/index.html'>$f</a></li>" >> versions/index.html
done
echo '</ul>' >> versions/index.html

git add . || die "unable to add for commit"
git commit -m "updated documentation for version $VERSION" || die "unable to commit to git"
git push origin gh-pages || die "unable to push to gh-pages"

cd $STAGE
rm -rf $TMP || die "unabel to clear $TMP"

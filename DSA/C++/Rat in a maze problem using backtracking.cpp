/* C++ program for rat in a maze problem */

#include<iostream>

using namespace std;

bool isSafe(int r,int c,int n,int **maze)

{

 if( r < 0 || r >= n )

 return false;

 if( c < 0 || c >= n )

 return false;

 if(maze[r][c])

 return true;

 return false;

}

bool solvemaze(int ** maze,int i,int j,int ** soln,int n)

{

 if(i == n-1 && j == n-1)

 {

 soln[i][j]=1;

 return true;

 }

 if(isSafe(i,j,n,maze))

 {

 soln[i][j]=1;

 if(solvemaze(maze,i+1,j,soln,n))

 return true;

 if(solvemaze(maze,i,j+1,soln,n))

 return true;

 soln[i][j]=0;

 }

 return false;

}

int main()

{

 int n;

 cin>>n;

 int** maze = new int*[n];

 for(int i = 0; i < n; ++i)

 maze[i] = new int[n];

 int** soln = new int*[n];

 for(int i = 0; i < n; ++i)

 soln[i] = new int[n];

 for(int i=0;i<n;i++)

 {

 for(int j=0;j<n;j++)

 {

 cin>>maze[i][j];

 soln[i][j]=0;

 }

 }

 if(solvemaze(maze,0,0,soln,n))

 {

 for(int i=0;i<n;i++)

 {

 for(int j=0;j<n;j++)

 cout<<soln[i][j]<<" ";

 cout<<endl;

 }

 }

 else

 {

cout<<"-1";

 }

}


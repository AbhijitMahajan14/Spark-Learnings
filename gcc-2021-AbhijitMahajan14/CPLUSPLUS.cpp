#include <iostream>
using namespace std;
int main() {
    long long n;
    cin>>n;
    if(n==1)
    cout<<n;
/*int x,y,z;
x=n/3;
y=x*2;
z=n-y;
cout<<x+z;

*/

for(long long i=1;i<n;i++)
{
    for(long long j=1;j<n/2;j++)
    {
        if(1*i+2*j==n&&abs(i-j)==1)
        {
            cout<<i+j;
        }
    }
}
}

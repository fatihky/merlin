#include "utils.h"

using namespace std;

void dumpStrVector(vector<string> vec) {
  int i = 0;

  for (auto &&str : vec) {
    cout << str;

    if (i != vec.size() - 1) {
      cout << ", ";
    }

    i++;
  }
}

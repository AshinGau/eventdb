#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>



char * DoubleS(double d){
    unsigned long long l;
    char* s = (char*)calloc(20, sizeof(char));
    *((double *)&l) = d;
    l = (l ^ (l >> 63 | 0x8000000000000000)) + 1;
    sprintf(s, "%llx", l);
    return s;
}

char * IntS(int d){
    unsigned int l;
    char* s = (char*)calloc(10, sizeof(char));
    *((int *)&l) = d;
    l = l ^ 0x80000000;
    sprintf(s, "%x", l);
    return s;
}

int SInt(char* str){
	unsigned int tmp;
	sscanf(str, "%x", &tmp);
	return tmp ^ 0x80000000;
}

double SDouble(char* str){
	unsigned long long l;
	sscanf(str, "%llx", &l);
	l -= 1;
	l = l ^ (!(l >> 63) | 0x8000000000000000);
	double tmp;
	*((unsigned long long*)&tmp) = l;
	return tmp;
}

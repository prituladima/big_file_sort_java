#include "com_prituladima_BigFileSorter.h"
#include <iostream>
#include <stdlib.h>
#include <stdio.h>

JNIEXPORT void JNICALL Java_com_prituladima_BigFileSorter_merge0
  (JNIEnv* env, jobject thisObject, jstring firstFileName, jstring secondFileName, jstring resultFileName) {

//    std::cout << "Merging started in C++" << std::endl;

    const char* first = env->GetStringUTFChars(firstFileName, NULL);
    const char* second = env->GetStringUTFChars(secondFileName, NULL);
    const char* result = env->GetStringUTFChars(resultFileName, NULL);

//    std::cout << "First: " << first << std::endl;
//    std::cout << "Second: " << second << std::endl;
//    std::cout << "Result: " << result << std::endl;
//https://stackoverflow.com/questions/7868936/read-file-line-by-line-using-ifstream-in-c
//https://riptutorial.com/c/example/8274/get-lines-from-a-file-using-getline--
//https://ideone.com/461DPt
//todo Make read and write buffered
FILE* fp1 = fopen(first, "r");
FILE* fp2 = fopen(second, "r");
FILE* fp3 = fopen(result, "w+");
//std::cout << "Readed file" << std::endl;
if (fp1 == NULL || fp2 == NULL)
    exit(EXIT_FAILURE);
//std::cout << "File exist" << std::endl;
char* line1 = NULL;
char* line2 = NULL;
size_t len1 = 0;//ssize_t line_size;
size_t len2 = 0;
ssize_t line_size1;
ssize_t line_size2;
line_size1 = getline(&line1, &len1, fp1);
line_size2 = getline(&line2, &len2, fp2);
while (line_size1 > -1 || line_size2 > -1) {
    // using printf() in all tests for consistency

       if (line_size2 == -1 || (line_size1 > -1 && std::string(line1) < std::string(line2))) {
           fprintf(fp3, line1);
//           std::cout << "Data from first file" << line1 << std::endl;
           //fprintf(fp3, "\n");
           line_size1 = getline(&line1, &len1, fp1);
       } else {
            fprintf(fp3, line2);
//            std::cout << "Data from second file" << line2 << std::endl;
//            std::cout <<  line2 << std::endl;
            //fprintf(fp3, "\n");
            line_size2 = getline(&line2, &len2, fp2);
       }

}
//if (valSecond == null || (valFirst != null && valFirst.compareTo(valSecond) < 0)) {
//                    writer.append(valFirst).append('\n');
//                    valFirst = scannerFirst.readLine();
//                } else {
//                    writer.append(valSecond).append('\n');
//                    valSecond = scannerSecond.readLine();
//                }
//                counter++;

fclose(fp1);
fclose(fp2);
fclose(fp3);
if (line1)
    free(line1);
    if (line2)
        free(line2);
//if (
remove(first);// == 0
//)
//      printf("Deleted successfully: f");
//   else
//      printf("Unable to delete the file");

//      if (
      remove(second);// == 0)
//            printf("Deleted successfully: s");
//         else
//            printf("Unable to delete the file");

    //return;
}

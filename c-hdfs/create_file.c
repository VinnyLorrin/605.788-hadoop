#include "hdfs.h" 

int main(int argc, char **argv) {
    hdfsFS fs = hdfsConnect("localhost", 8020);
    if (!fs) {
          fprintf(stderr, "hdfsConnect returned null\n");
          exit (1);
    }

    const char* writePath = "/c-testfile.txt";
    hdfsFile writeFile = hdfsOpenFile(fs, writePath, O_WRONLY|O_CREAT, 0, 0, 0);
    if(!writeFile) {
          fprintf(stderr, "Failed to open %s for writing!\n", writePath);
          exit(2);
    }
    char* buffer = "Hello, World!\n";
    tSize num_written_bytes = hdfsWrite(fs, writeFile, (void*)buffer, strlen(buffer)+1);
    if (hdfsFlush(fs, writeFile)) {
           fprintf(stderr, "Failed to 'flush' %s\n", writePath); 
          exit(3);
    }
   hdfsCloseFile(fs, writeFile);
}

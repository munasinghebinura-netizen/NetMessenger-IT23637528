
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>

#define USERNAME_SIZE 50
#define GREEN "\033[32m"
#define RED "\033[31m"
#define BLUE "\033[34m"
#define YELLOW "\033[33m"
#define RESET "\033[0m"
#define SERVER_IP "127.0.0.1"
#define PORT 14528
#define BUFFER_SIZE 4096

int client_socket;

int receive_line_from_socket(int socket,
                             char *line,
                             size_t line_size,
                             char *pending,
                             size_t *pending_len)
{
    while (1)
    {
        for (size_t i = 0; i < *pending_len; i++)
        {
            if (pending[i] == '\n')
            {
                size_t length = i + 1;

                if (length >= line_size)
                {
                    return -1;
                }

                memcpy(line, pending, length);
                line[length] = '\0';

                memmove(pending,
                        pending + length,
                        *pending_len - length);

                *pending_len -= length;

                return 1;
            }
        }

        if (*pending_len >= BUFFER_SIZE)
        {
            return -1;
        }

        ssize_t received = recv(socket,
                                 pending + *pending_len,
                                 BUFFER_SIZE - *pending_len,
                                 0);

        if (received <= 0)
        {
            return 0;
        }

        *pending_len += (size_t)received;
    }
}
int receive_exact_bytes(int socket,
                        char *pending,
                        size_t *pending_len,
                        size_t length,
                        FILE *file)
{
    size_t total_received = 0;

    while (total_received < length)
    {
        if (*pending_len > 0)
        {
            size_t available = *pending_len;
            size_t remaining = length - total_received;
            size_t to_write = available < remaining
                              ? available
                              : remaining;

            if (fwrite(pending, 1, to_write, file) != to_write)
            {
                return -1;
            }

            total_received += to_write;

            memmove(pending,
                    pending + to_write,
                    *pending_len - to_write);

            *pending_len -= to_write;

            continue;
        }

        char file_buffer[BUFFER_SIZE];

        size_t remaining = length - total_received;
        size_t to_read = remaining < sizeof(file_buffer)
                         ? remaining
                         : sizeof(file_buffer);

        ssize_t received = recv(socket,
                                 file_buffer,
                                 to_read,
                                 0);

        if (received <= 0)
        {
            return -1;
        }

        if (fwrite(file_buffer,
                   1,
                   (size_t)received,
                   file) != (size_t)received)
        {
            return -1;
        }

        total_received += (size_t)received;
    }

    return 0;
}
void *receive_messages(void *arg)
{
    char buffer[BUFFER_SIZE];
    char pending[BUFFER_SIZE * 2];
size_t pending_len = 0;

    (void)arg;

    while (1)
    {
                int line_status = receive_line_from_socket(client_socket,
                                                    buffer,
                                                    sizeof(buffer),
                                                    pending,
                                                    &pending_len);

        if (line_status <= 0)
        {
            printf("\nDisconnected from server.\n");
            break;
        }

        buffer[strcspn(buffer, "\r\n")] = '\0';
                 if (strncmp(buffer, "FILE ", 5) == 0)
        {
            char filename[BUFFER_SIZE];
            long filesize;

            if (sscanf(buffer + 5, "%s %ld",
                       filename, &filesize) == 2 &&
                filesize > 0)
            {
                FILE *file = fopen(filename, "wb");

                if (file == NULL)
                {
                    printf("\nFailed to create received file: %s\n",
                           filename);
                }
                else
                {
                    if (receive_exact_bytes(client_socket,
                                            pending,
                                            &pending_len,
                                            (size_t)filesize,
                                            file) == 0)
                    {
                        fclose(file);

                        printf("\nFile received: %s (%ld bytes)\n",
                               filename,
                               filesize);
                    }
                    else
                    {
                        fclose(file);
                        remove(filename);

                        printf("\nFile receive failed: %s\n",
                               filename);
                    }
                }

                printf("> ");
                fflush(stdout);
                continue;
            }
        }

       if (strncmp(buffer, "MSG PRIV ", 9) == 0)
{
    char sender[USERNAME_SIZE];
    char *message = strchr(buffer + 9, ' ');

    if (message != NULL)
    {
        *message = '\0';
        strcpy(sender, buffer + 9);
        message++;

        printf("\n" RED "%s: %s" RESET "\n", sender, message);
    }
}
else if (strncmp(buffer, "MSG ROOM ", 9) == 0)
{
    char room[USERNAME_SIZE];
    char sender[USERNAME_SIZE];
    char *message = buffer + 9;

    sscanf(message, "%s %s", room, sender);

    message = strchr(message, ' ');
    if (message != NULL)
    {
        message++;
        message = strchr(message, ' ');

        if (message != NULL)
        {
            message++;
            printf("\n" BLUE "[%s] %s: %s" RESET "\n",
       room, sender, message);
        }
    }
}
else if (strncmp(buffer, "MSG BCAST ", 10) == 0)
{
    char sender[USERNAME_SIZE];
    char *message = strchr(buffer + 10, ' ');

    if (message != NULL)
    {
        *message = '\0';
        strcpy(sender, buffer + 10);
        message++;

        printf("\n" GREEN "[BROADCAST] %s: %s" RESET "\n",
       sender, message);
    }
}
else
{
    printf("\n" YELLOW "%s" RESET "\n", buffer);
}

printf("> ");
fflush(stdout);
    }

    return NULL;
}
int send_all(int socket, const void *data, size_t length)
{
    size_t total_sent = 0;

    while (total_sent < length)
    {
        ssize_t sent = send(socket,
                            (const char *)data + total_sent,
                            length - total_sent,
                            0);

        if (sent <= 0)
        {
            return -1;
        }

        total_sent += sent;
    }

    return 0;
}


int main(void)
{
    struct sockaddr_in server_addr;
    pthread_t receiver_thread;
    char username[100];
    char command[BUFFER_SIZE];

    client_socket = socket(AF_INET, SOCK_STREAM, 0);

    if (client_socket < 0)
    {
        perror("socket");
        return EXIT_FAILURE;
    }

    memset(&server_addr, 0, sizeof(server_addr));

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);

    if (inet_pton(AF_INET,
                  SERVER_IP,
                  &server_addr.sin_addr) <= 0)
    {
        perror("inet_pton");
        close(client_socket);
        return EXIT_FAILURE;
    }

    if (connect(client_socket,
                (struct sockaddr *)&server_addr,
                sizeof(server_addr)) < 0)
    {
        perror("connect");
        close(client_socket);
        return EXIT_FAILURE;
    }

    printf("Connected to NetMessenger server.\n");
    printf("Server: %s:%d\n", SERVER_IP, PORT);

    printf("Enter username: ");
    fflush(stdout);

    if (fgets(username, sizeof(username), stdin) == NULL)
    {
        close(client_socket);
        return EXIT_FAILURE;
    }

    username[strcspn(username, "\n")] = '\0';

    snprintf(command,
             sizeof(command),
             "REGISTER %s\n",
             username);

    if (send(client_socket,
             command,
             strlen(command),
             0) < 0)
    {
        perror("send");
        close(client_socket);
        return EXIT_FAILURE;
    }

    if (pthread_create(&receiver_thread,
                       NULL,
                       receive_messages,
                       NULL) != 0)
    {
        perror("pthread_create");
        close(client_socket);
        return EXIT_FAILURE;
    }

    pthread_detach(receiver_thread);

    printf("Type commands below.\n");
    printf("> ");
    fflush(stdout);

    while (1)
    {
        if (fgets(command, sizeof(command), stdin) == NULL)
        {
            break;
        }        if (strncmp(command, "SENDFILE ", 9) == 0)
        {
            char target[100];
            char filename[256];
            long filesize;

            if (sscanf(command + 9, "%99s %255s %ld",
                       target, filename, &filesize) != 3)
            {
                printf("Usage: SENDFILE <target> <filename> <filesize>\n");
                printf("> ");
                fflush(stdout);
                continue;
            }

            FILE *file = fopen(filename, "rb");

            if (file == NULL)
            {
                perror("File open");
                printf("> ");
                fflush(stdout);
                continue;
            }

            fseek(file, 0, SEEK_END);
            long actual_size = ftell(file);
            fseek(file, 0, SEEK_SET);

            if (actual_size != filesize)
            {
                printf("File size mismatch. Actual: %ld bytes\n",
                       actual_size);
                fclose(file);
                printf("> ");
                fflush(stdout);
                continue;
            }

            if (send_all(client_socket,
                         command,
                         strlen(command)) < 0)
            {
                perror("send");
                fclose(file);
                break;
            }

            char file_buffer[BUFFER_SIZE];
            size_t bytes_read;

            while ((bytes_read = fread(file_buffer,
                                       1,
                                       sizeof(file_buffer),
                                       file)) > 0)
            {
                if (send_all(client_socket,
                             file_buffer,
                             bytes_read) < 0)
                {
                    perror("send file");
                    
                    break;
                }
            }

            fclose(file);

            printf("File sent: %s (%ld bytes)\n",
                   filename, filesize);

            printf("> ");
            fflush(stdout);
            continue;
        }


        if (send(client_socket,
                 command,
                 strlen(command),
                 0) < 0)
        {
            perror("send");
            break;
        }

        if (strncmp(command, "QUIT", 4) == 0)
        {
           sleep(1);
            break;
        }

        printf("> ");
        fflush(stdout);
    }

    close(client_socket);

    return 0;
}

#ifndef SERVER_H
#define SERVER_H

/**
 * Starts a Redis-like telnet server on the specified port number.
 * This server allows clients to connect via telnet and process commands.
 * 
 * @param port The port number on which the server will listen (default: 6379)
 * @return 0 if successful, 1 if failed
 */
int server_init(int port);

#endif // SERVER_H 
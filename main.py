#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import with_statement

import os
import sys
import errno, threading
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time


import httplib2
import apiclient.discovery
import apiclient.http
import oauth2client.client


from fuse import FUSE, FuseOSError, Operations

credential_path = os.getenv("HOME")+"/.gauth"

class Passthrough(Operations):

    def path_to_id(self, path):
        path = self.root + path
        try:
            dir_obj = self.path_table[path]
            if dir_obj:
                return dir_obj
        except:
            self.dblock.acquire()        
            plist = path.split("/")        
            dir_obj = self.drive.files().get(fileId='root').execute()
            
            for filename in plist:
                dir_result = []
                if filename == '':
                    continue
                param = {'q' : "'%s' in parents and title = '%s'" % (dir_obj['id'], filename)}
                files = self.drive.files().list(**param).execute()
                dir_result.extend(files['items'])
                if len(dir_result) == 0:
                    dir_obj = None
                    break
                dir_obj = dir_result[0]        
                
            self.dblock.release()
            if dir_obj:
                self.path_table[path] = dir_obj
            return dir_obj
    def create_folder(self, path):
        basename = os.path.basename(path)
        dirname = os.path.dirname(path)
        print "dirname:[", dirname, "] basename:[", basename, "]"
        dir_folder = self.path_to_id(dirname)
        base_folder = self.path_to_id(path)
        if dir_folder and base_folder == None:
            body = {
              'title': basename,
              'description': "0",
              'mimeType': 'application/vnd.google-apps.folder',
              'psize': 0              
            }
            body['parents'] = [{'id': dir_folder['id']}]
            self.dblock.acquire()
            base_folder = self.drive.files().insert(body=body).execute()
            self.dblock.release()
        return base_folder

    def create_file(self, path):
        basename = os.path.basename(path)
        dirname = os.path.dirname(path)
        print "dirname:[", dirname, "] basename:[", basename, "]"
        dir_folder = self.path_to_id(dirname)
        base_folder = self.path_to_id(path)
        media_body = apiclient.http.MediaInMemoryUpload(body="")
        if dir_folder and base_folder == None:
            body = {
              'title': basename,
              'description': "file-desc",
#               'mimeType': 'application/vnd.google-apps.folder'
            }
            body['parents'] = [{'id': dir_folder['id']}]
            self.dblock.acquire()
            base_folder = self.drive.files().insert(body=body, media_body=media_body).execute()
            self.dblock.release()
            
        return base_folder
        
            
    def __init__(self, root):
        self.fd = 5
        self.fd_table = dict()
        self.path_table = dict()
        self.dir_table = dict()
        self.attr_table = dict()
        self.root = root
        self.dblock = threading.Lock()
        
        # OAuth 2.0 scope that will be authorized.
        # Check https://developers.google.com/drive/scopes for all available scopes.
        OAUTH2_SCOPE = 'https://www.googleapis.com/auth/drive'
        
        # Location of the client secrets.
        CLIENT_SECRETS = 'client_secrets.json'
        
        
        if os.path.exists(credential_path):
            f = open(credential_path, 'r')        
            credentials = oauth2client.client.OAuth2Credentials.new_from_json(f.read())
            f.close()
        else:    
            # Perform OAuth2.0 authorization flow.        
            flow = oauth2client.client.flow_from_clientsecrets(CLIENT_SECRETS, OAUTH2_SCOPE)
            flow.redirect_uri = oauth2client.client.OOB_CALLBACK_URN        
            authorize_url = flow.step1_get_authorize_url()
            print 'Go to the following link in your browser: ' + authorize_url                
            code = raw_input('Enter verification code: ').strip()
    #         code = "4/ARN2EquD1lM8Ycs6l4-c1fPIo_EUNN30X1IZO35voTc.Uj5QSXkTD38agrKXntQAax2-I87gmgI"
            credentials = flow.step2_exchange(code)
            f = open(credential_path, 'w')
            f.write(credentials.to_json())
            f.close()
        
        # Create an authorized Drive API client.
        http = httplib2.Http()
        credentials.authorize(http)
        self.drive = apiclient.discovery.build('drive', 'v2', http=http)
        self.create_folder('/')



    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path
        
    
    def get_children(self, path):
        folder = self.path_to_id(path)

        dir_result = []
        param = {'q' : "'%s' in parents" % (folder['id'])}        
        self.dblock.acquire()
        children = self.drive.files().list(**param).execute()
        self.dblock.release()
        dir_result.extend(children['items'])
#         print "get_children", dir_result
        return dir_result
    def update_filelength(self, path, length):
        self.attr_table[path]['size'] = length
        file = self.path_to_id(path)
        if file:
            file['description'] =  str(length)
            self.dblock.acquire()            
            self.drive.files().update(body=file, fileId=file['id']).execute()
            self.dblock.release()

    # Filesystem methods
    # ==================

#     def access(self, path, mode):
#         full_path = self._full_path(path)
#         print "access", path, mode

#     def chmod(self, path, mode):
#         full_path = self._full_path(path)
#         return os.chmod(full_path, mode)
# 
#     def chown(self, path, uid, gid):
#         full_path = self._full_path(path)
#         return os.chown(full_path, uid, gid)
    def getattr(self, path, fh=None):
        print "getattr", path
        try:
            file_obj = self.attr_table[path]
            return file_obj
        except:
            if path == "/":
                self.attr_table[path] = dict(st_mode=(S_IFDIR | 0700), st_ctime=time(), st_mtime=time(), st_atime=time(), st_nlink=2)            
                return  self.attr_table[path]
            else:            
                file_obj = self.path_to_id(path)
                if file_obj:
                    if file_obj['mimeType'] == 'application/vnd.google-apps.folder':
                        size = int(file_obj['description'])
                        mode = S_IFREG
                        #size = int(file_obj['fileSize'])
                        
                        self.attr_table[path] = dict(st_mode=(mode | 0600), st_nlink=1,
                                    st_size=size, st_ctime=time(), st_mtime=time(),
                                    st_atime=time(), st_uid=os.getuid(), st_gid=os.getgid())
                    return self.attr_table[path]
                else:
                    #self.attr_table[path] = dict()
                    raise FuseOSError(errno.ENOENT) 
                    #return self.attr_table[path]

    def readdir(self, path, fh):
        print "readdir", path
        try:
            result = self.dir_table[path]
            for file1 in result:                            
                yield file1['title']
        except:        
            result = self.get_children(path)
            self.dir_table[path] = result
            yield "."
            yield ".."
            for file1 in result:
                print file1['title']
                yield file1['title']
 

#     def readlink(self, path):
#         pathname = os.readlink(self._full_path(path))
#         if pathname.startswith("/"):
#             # Path name is absolute, sanitize it.
#             return os.path.relpath(pathname, self.root)
#         else:
#             return pathname

#     def mknod(self, path, mode, dev):
#         return os.mknod(self._full_path(path), mode, dev)
# 
#     def rmdir(self, path):
#         full_path = self._full_path(path)
#         return os.rmdir(full_path)

#     def mkdir(self, path, mode):
#         return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
#         print "statfs", path
        
#         full_path = self._full_path(path)
#         stv = os.statvfs(full_path)
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

#     def unlink(self, path):
#         return os.unlink(self._full_path(path))
#  
#     def symlink(self, name, target):
#         return os.symlink(name, self._full_path(target))
#  
#     def rename(self, old, new):
#         return os.rename(self._full_path(old), self._full_path(new))
#  
#     def link(self, target, name):
#         return os.link(self._full_path(target), self._full_path(name))
#  
#     def utimens(self, path, times=None):
#         return os.utime(self._full_path(path), times)
#  
    # File methods
    # ============

    def open(self, path, flags):        
        print "open", path
        filename = self.path_to_id(path)
        if filename:
            try:
                fd = self.fd_table[path]
                return fd
            except:
                self.fd_table[filename['id']] = self.fd
                self.fd += 1
                return self.fd_table[filename['id']]
        else:
            return 0

    def create(self, path, mode, fi=None):
        print "create[", path,"]"
#         full_path = self._full_path(path)
#         return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)
#         media_body = apiclient.http.MediaInMemoryUpload(None, mimetype='application/vnd.google-apps.folder')
#         body = {
#           'title': 'disk0',
#           'description': "disk0-desc",
#           'mimeType': 'application/vnd.google-apps.folder'
#         }        
        # Set the parent folder.      
        
        
#         file = self.drive.files().insert(
#             body=body,
#             media_body=media_body).execute()
#         print file
        file = self.create_folder(path)

        self.fd_table[file['id']] = self.fd
        self.fd += 1
        return self.fd_table[file['id']]


    def read(self, path, length, offset, fh):
        print "read", path
        lba = offset / 4096
        filename = path+"/"+str(lba).zfill(32)
        file = self.create_file(filename)
        self.dblock.acquire()
        data = self.drive.files().get_media(fileId=file['id']).execute()
        self.dblock.release()
        return data
#         print path
#         os.lseek(fh, offset, os.SEEK_SET)
#         return os.read(fh, length)
# 
    def write(self, path, buf, offset, fh):
        print "write", path, offset, len(buf)
        lba = offset / 4096
        filename = path+"/"+str(lba).zfill(32)
        
        file = self.create_file(filename)
        
        dirname = self.path_to_id(path)
        length = int(dirname['description'])        
        
        if offset+len(buf) > length:
            dirname['description'] = str(offset+len(buf))
            try:
                del self.attr_table[path]
            except:
                pass
            self.dblock.acquire()
            self.drive.files().update(body=dirname, fileId=dirname['id']).execute()
            self.dblock.release()
        
        media_body = apiclient.http.MediaInMemoryUpload(body=buf)
        
        self.dblock.acquire()
        self.drive.files().update(body=file, fileId=file['id'], media_body=media_body).execute()
        self.dblock.release()        
        return len(buf)
#         print path
#         os.lseek(fh, offset, os.SEEK_SET)
#         return os.write(fh, buf)
# 
    def truncate(self, path, length, fh=None):
        print "truncate", path, length
        self.update_filelength(path, length)
        
        
    def unlink(self, path):
        filename = self.path_to_id(path)
        self.dblock.acquire()
        print "unlink", path
        if self.fd_table.has_key(path):
            del self.fd_table[path]
        if self.path_table.has_key(path):
            del self.path_table[path]        
        if self.dir_table.has_key(path):
            del self.dir_table[path]
        if self.attr_table.has_key(path):
            del self.attr_table[path]
        
        self.drive.files().delete(fileId=filename['id']).execute()
        self.dblock.release()
        
        
# 
#     def flush(self, path, fh):
#         return os.fsync(fh)
# 
#     def release(self, path, fh):
#         return os.close(fh)
# 
#     def fsync(self, path, fdatasync, fh):
#         return self.flush(path, fh)


def main(mountpoint, root):
    FUSE(Passthrough(root), mountpoint, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])

package main

import (
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/jlaffaye/ftp"

	log "github.com/alex-mj-pradius/fox-log"
)

/*
Реализуем потоки между заданными локальными и ftp папками.

я
Алгоритм:
1. 	из файла настроек выбрать следующий поток
2. 	по маскам имен файлов сформировать список файлов из SOURCE
3. 	для каждого файла из списка произвести тест захвата	(напр. двойное переименование),
	в случае успеха	приступить к копированию файлов по списку DESTINATIONS (данного потока)
	(копируем на получателя с именем *.*XXX, в случае удачи - переименовываем)
	после копирования во все папки назначения, удаляем исходный в SOURSE (<deleteFromSorce>1</deleteFromSorce>)
*/

//Log to all function
var Log log.Log

// Settings - settings from xml-file
var Settings settings

func main() {

	Settings = settings{}
	/////////////////	init log ///////////////////////////////////////////
	Settings.ServiceName = "ftp-flows"
	Log.Start(Settings.ServiceName)

	///////////////   get settings	//////////////////////////////////////
	Settings.getFromXMLfile()
	///////////////   MAIN 	 //////////////////////////////////////
	for _, flow := range Settings.Flows {

		fmt.Println(flow)

		flow.openFTPConnects()
		flow.getFilesFromSorces()
		flow.copyFilesToDestinayions()
		flow.deleteFilesfromSource()
		flow.closeFTPConnects()
	}
	Log.End()
}

type settings struct {
	ServiceName string
	ID          string `xml:"id,attr"`    // sample id (не используется)
	Flows       []flow `xml:"flows>flow"` // потоки из одного source в несколько (или один) destinations

}

type flow struct {
	/*
	   	<flow>
	          <deleteFromSorce comment="0 - оставить в source, 1 - удалить (в случае удачного по всем destinations)">0</deleteFromSorce>
	          <source comment="каталог - источник файлов">  ......  </source>
	          <destinations comment="каталоги назначения - куда будем копировать">
	              <destination>	......	</destination>
	              <destination>	......  </destination>
	          </destinations>
	      </flow>
	*/
	DeleteFromSorce string                  `xml:"deleteFromSorce"` // 0 - оставить в source, 1 - удалить (в случае удачного по всем destinations)
	LocalTempDir    string                  `xml:"local-temp-dir"`
	FileMasks       []string                `xml:"fileMasks>fileMask"` // список масок к переносу
	Files           map[fileFromSource]bool //[]string
	Source          flowDir                 `xml:"source"`                   // - папка из которой нужно зачитывать (она одна, нужно больше - пиши другой flow)
	Destinations    []flowDir               `xml:"destinations>destination"` // - папки (много) в которые нужно скопировать
}

type fileFromSource struct {
	FileName     string
	TempName     string
	OnlyFileName string
}

//getFileListFromSource - мутная функция вытаскивает имена файлов из источника
func (flow *flow) getFileListFromSource(fileMask string) []string {
	var fileList []string
	if flow.Source.LocalDir == "" { // FTP FTP FTP FTP FTP FTP FTP FTP FTP FTP
		entries, _ := flow.Source.FtpClient.List(flow.Source.FtpSettings.FtpDir)
		for _, entrie := range entries {
			if ok, _ := filepath.Match(fileMask, entrie.Name); ok == true {
				fileList = append(fileList, entrie.Name)
			}
		}
	} else { // LOCAL LOCAL LOCAL LOCAL LOCAL LOCAL LOCAL LOCAL LOCAL LOCAL
		localFiles, err := filepath.Glob(flow.Source.LocalDir + fileMask)
		if err != nil {
			Log.Error(err.Error())
		}
		for _, fileName := range localFiles {

			fileList = append(fileList, fileName)
		}
	}
	return fileList
}

// getFilesFromSorces - append fileName, TempName to flow.Files, copy file to temp
func (flow *flow) getFilesFromSorces() {
	flow.Files = make(map[fileFromSource]bool)
	for _, fileMask := range flow.FileMasks {
		for _, fileName := range flow.getFileListFromSource(fileMask) {
			var file fileFromSource
			file.FileName = fileName
			_, file.OnlyFileName = filepath.Split(fileName)
			var err error
			file.TempName, err = flow.copyToTemp(file.OnlyFileName)
			if err == nil {
				flow.Files[file] = true
			}
		}
	}
	// debug:
	fmt.Println(flow.Files)
}

//func (flow *flow) копироватьФайлИзСоурсаВТемпДиректорию()  {
func (flow *flow) copyToTemp(fileName string) (string, error) {

	// TODO: rename -> *.*.access-test, rename -> *.*
	//   if flow.accessTest(fileName)

	var fileReader io.Reader
	// get link to file: open file if local, Retr file if FTP
	if flow.Source.LocalDir == "" {
		localFile, err := flow.Source.FtpClient.Retr(flow.Source.FtpSettings.FtpDir + fileName)
		defer localFile.Close()
		if err != nil {
			Log.Error(err.Error())
			return "", err
		}
		fileReader = localFile
	} else {
		localFile, err := os.Open(flow.Source.LocalDir + fileName)
		//localFile, err := os.Open(fileName)
		defer localFile.Close()
		if err != nil {
			Log.Error(err.Error())
			return "", err
		}
		fileReader = localFile
	}

	destinationPath := flow.LocalTempDir + fileName
	file, err := os.Create(destinationPath)
	if err != nil {
		Log.Error(err.Error())
		return "", err
	}

	_, err = io.Copy(file, fileReader)
	if err != nil {
		Log.Error(err.Error())
		return "", err
	}
	Log.Info(flow.Source.LocalDir + flow.Source.FtpSettings.FtpDir + "/" + fileName + " --toTemp-> " + destinationPath)

	file.Close()
	return destinationPath, nil
}

//
func (flow *flow) copyFilesToDestinayions() {
	for file, succes := range flow.Files {
		for _, dest := range flow.Destinations {
			flow.Files[file] = succes && flow.copyFileTo(&dest, file)
		}
	}
}

func (flow *flow) copyFileTo(dest *flowDir, fileSrc fileFromSource) bool {

	// TODO расписать все варианты что откуда может копироваться
	// (файлы из источника уже в flow.Files.TempName, получателей может быть много)
	/*
		if dest.FtpClient == nil {
			fmt.Println("	* Подключиться к FTP " + dest.FtpSettings.Server + " * ")
			var err error
			dest.FtpClient, err = ftp.Dial(dest.FtpSettings.Server)
			if err != nil {
				Log.Error("(#94) Не могу подключиться к FTP, [server:" + dest.FtpSettings.Server + "] " + err.Error())
				return false
			}
			if err := dest.FtpClient.Login(dest.FtpSettings.Login, dest.FtpSettings.Password); err != nil {
				Log.Error("(#94) Не могу подключиться к FTP, [server:" + dest.FtpSettings.Server + "] " + err.Error())
				return false
			}
		}
	*/
	file, err := os.Open(fileSrc.TempName)
	defer file.Close()
	if err != nil {
		Log.Error("#208" + err.Error())
		return false
	}
	if dest.LocalDir == "" {
		destFtpName := dest.FtpSettings.FtpDir + fileSrc.OnlyFileName
		fmt.Println(">>>>>>>")
		fmt.Println(destFtpName)
		fmt.Println(destFtpName)
		fmt.Println(dest.FtpClient)

		err = dest.FtpClient.Stor(destFtpName, file)
		if err != nil {
			Log.Error("#215" + err.Error())
			return false
		}
		Log.Info(fileSrc.TempName + " ---> " + destFtpName)
	} else {
		destLocalName := dest.LocalDir + fileSrc.OnlyFileName
		copyFile(fileSrc.TempName, destLocalName)
		//Log.Info(fileSrc.TempName + " ---> " + destLocalName)
	}

	return true
}

func (flow *flow) deleteFilesfromSource() {
	for file, succes := range flow.Files {
		if (flow.DeleteFromSorce == "1") && succes {
			flow.deleteFile(file)
		}
		//удалять также файл в темп каталоге
		deleteFile(file.TempName)
	}

}

func (flow *flow) deleteFile(file fileFromSource) {
	if flow.Source.LocalDir == "" { //FTP
		flow.Source.FtpClient.Delete(flow.Source.FtpSettings.FtpDir + file.OnlyFileName)
	} else { // local
		deleteFile(file.FileName)
	}
}

func (flow *flow) accessTest(fileName string) {
	// тест доступности файла путем переименования (ну и переименуем назад - если что-то пойдет не так, чтобы он снова попал под маску в следующий раз)
}

/*
	<local-dir comment="локальный каталог, если заполнен, то ftp настройки игнорируем">/host/in/</local-dir>
	<ftp-settings>
		<server>192.168.110.10:21</server>
		<login>pradttst</login>
		<password>pradttst!@#</password>
		<ftp-dir>/host/in/</ftp-dir>
	</ftp-settings>
*/

type flowDir struct {
	LocalDir string `xml:"local-dir"` // локальный каталог, если заполнен, то ftp настройки игнорируем

	FtpSettings ftpSettings `xml:"ftp-settings"` // фтп настройки
	FtpClient   *ftp.ServerConn
}

////////////// FTP connects ///////////////////////////
//openFTPConnects() - make connects to ftp
func (flow *flow) openFTPConnects() {
	// one SOURCE
	if flow.Source.LocalDir == "" {
		if flow.Source.FtpClient == nil {
			flow.Source.FtpClient = flow.Source.FtpSettings.connect()
		}
	}
	// multi DESTINATIONS
	for _, dest := range flow.Destinations {
		if dest.LocalDir == "" {
			if dest.FtpClient == nil {
				dest.FtpClient = dest.FtpSettings.connect()
			}
		}
	}
}

//openFTPConnects() - make connects to ftp
func (flow *flow) closeFTPConnects() {
	// закрыть коннекты к FTP
	if flow.Source.FtpClient != nil {
		err := flow.Source.FtpClient.Quit()
		if err != nil {
			Log.Error(err.Error())
		}
	}
	for _, dest := range flow.Destinations {
		if dest.FtpClient != nil {
			err := dest.FtpClient.Quit()
			if err != nil {
				Log.Error(err.Error())
			}
		}
	}
}

//	ftpSettings.connect()  !PANIC
func (ftpSettings *ftpSettings) connect() *ftp.ServerConn {

	fmt.Println("	* Подключиться к FTP " + ftpSettings.Server + " * ")
	FTPClient, err := ftp.Dial(ftpSettings.Server)
	if err != nil {
		Log.Error("(#123) Не могу подключиться к FTP, [server:" + ftpSettings.Server + "] " + err.Error())
		panic("A way out. FTP is not available.")
	}
	if err := FTPClient.Login(ftpSettings.Login, ftpSettings.Password); err != nil {
		Log.Error("(#127) Не могу подключиться к FTP, [server:" + ftpSettings.Server + "] " + err.Error())
		panic("A way out. FTP is not available.")
	}
	return FTPClient
}

//////////////////// S E T T I N G S ////////////////////
type ftpSettings struct {
	Server   string `xml:"server"`   // 192.168.110.10:21
	Login    string `xml:"login"`    // login to data base
	Password string `xml:"password"` // password to data base
	FtpDir   string `xml:"ftp-dir"`  // ftp-dir если нужно забирать не с корневого
}

func (setingsFromXML *settings) getFromXMLfile() {

	// Open our Settings: CrossDriverSettings.xml
	// пробуем открыть файл с настройками из каталога с программой
	ex, err := os.Executable() // workDir, err := os.Getwd()
	if err != nil {
		Log.Error("(#settingsXML.getFromXMLfile() #1) провалилась попытка получить рабочую директорию (os.Executable()) :" + err.Error())
		panic(err)
	}
	//Log.Info(ex)
	exPath := filepath.Dir(ex)

	settingFileName := Settings.ServiceName + "-settings.xml"
	xmlFile, err := os.Open(exPath + "/" + settingFileName)

	if err != nil {
		Log.Error(err.Error())
		// не получилось открываем константный, отладка
		//debugFileName := "/home/alex/go/src/FlowTP/" + settingFileName
		debugFileName := "/Users/alex/go/src/ftp-flows/" + settingFileName
		Log.Info("... " + settingFileName + " не найден в папке с исполняемым файлом, открываем " + debugFileName)
		xmlFile, err = os.Open(debugFileName)
		if err != nil {
			Log.Error("342# error os.Open(" + debugFileName + "): " + err.Error())
		}
	}
	defer xmlFile.Close()

	byteXML, err := ioutil.ReadAll(xmlFile)
	if err != nil {
		Log.Error("error ReadAll( " + settingFileName + "): " + err.Error())
	}
	if err := xml.Unmarshal(byteXML, &setingsFromXML); err != nil {
		Log.Error("error xml.Unmarshal " + settingFileName + ":" + err.Error())
		panic(err)
	}

}

///////////////////////// T O O L S /////////////////////
func deleteFile(src string) error {

	//проверяем доступ к файлу
	f, err := os.OpenFile(src, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}

	//удаляем
	err = os.Remove(src)
	if err != nil {
		if os.IsNotExist(err) {
			//log.WithFields(logrus.Fields{"src": src, "dst": dst, "err": err}).Warn("Файл перемещён, но удалить его не получилось: удалён кем-то ещё.")
			fmt.Println("Файл перемещён, но удалить его не получилось: удалён кем-то ещё.")
			return nil
		}
		return fmt.Errorf("Файл перемещён. Но удалить его не получилось. %v", err)
	}
	Log.Info("DELETE file:  --->>> " + src)
	return nil
}

//копируем файл
func copyFile(src string, dst string) (err error) {

	sourcefile, err := os.Open(src)
	if err != nil {
		Log.Error("Не могу открыть файл:" + src + "\n" + err.Error())
		return err
	}
	defer sourcefile.Close()

	destfile, err := os.Create(dst)
	if err != nil {
		Log.Error("Не могу создать файл:" + dst + "\n" + err.Error())
		return err
	}
	//копируем содержимое и проверяем коды ошибок
	_, err = io.Copy(destfile, sourcefile)
	if closeErr := destfile.Close(); err == nil {
		//если ошибки в io.Copy нет, то берем ошибку от destfile.Close(), если она была
		err = closeErr
	}
	if err != nil {
		Log.Error("Не могу скопировать файл:" + src + " в " + dst + "\n" + err.Error())
		return err
	}
	sourceinfo, err := os.Stat(src)
	if err == nil {
		err = os.Chmod(dst, sourceinfo.Mode())
	}
	Log.Info(src + " --- copy to --- >>> " + dst)
	return err
}

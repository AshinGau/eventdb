import ROOT
import os

class events:
	def __init__(self, filePath, fromRoot = True):
		self.f = ROOT.TFile(filePath)
		self.t = self.f.Get('ntTAG' if fromRoot else 'Event')

	def __enter__(self):
		return self

	def __exit__(self, type, value, traceback):
		self.close()

	def getEntries(self):
		return self.t.GetEntries()

	@classmethod
	def showEntries(cls, dirPath):
		total = 0
		for walker in os.walk(dirPath):
			root, dirs, files = walker
			if files:
				count = 0
				runID = ''
				for file in files:
					with events(os.path.join(root, file)) as evts:
						count += evts.getEntries()
						runID = evts.getRunNo()
				total += count
				print 'run %s has %d event(s)'%(runID, count)
		print 'total event(s): %d'%total

	def getRunNo(self, entryID = 0):
		self.t.GetEntry(entryID)
		return str(self.t.runNo)

	def close(self):
		self.f.Close()
